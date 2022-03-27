#![deny(missing_docs)]

//! A Stream for executing retryable async operations until they succeed
//! using dynamic concurrency limits to find an optimal throughput.

use futures::future::BoxFuture;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::{Stream, StreamExt};
use std::collections::VecDeque;
use std::default::Default;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Something that can repeatedly generate futures to be tried until one succeeds.
/// The futures must return a Result so that the stream knows if they succeed or fail.
pub trait Retryable: Unpin + Sized {
    /// The type for the `Ok` case.
    type Ok;
    /// The type for the `Err` case.
    type Err;

    /// Run the future. On Err, this method returns a `Self` which will be used to retry.
    /// Normally, this should return `self`, but you can construct a new `Self` type if
    /// you with.
    fn run<'a>(self) -> BoxFuture<'a, Result<Self::Ok, (Self::Err, Self)>> where Self: 'a;
}

impl<Func, Fut, Ok, Err> Retryable for Func
where
    Func: Unpin + Send + Sync + Fn() -> Fut,
    Fut: Future<Output = Result<Ok, Err>> + Send + Sync,
{
    type Ok = Ok;
    type Err = Err;

    fn run<'a>(self) -> BoxFuture<'a, Result<Self::Ok, (Self::Err, Self)>> where Self: 'a {
        Box::pin(async move {
            let result = (self)().await;
            match result {
                Ok(ok) => Ok(ok),
                Err(err) => Err((err, self)),
            }
        })
    }
}

impl<Func, Fut, Args, Ok, Err> Retryable for (Func, Args)
where
    Func: Unpin + Send + Sync + Fn(Args) -> Fut,
    Fut: Future<Output = Result<Ok, (Err, Args)>> + Send + Sync,
    Args: Unpin + Send + Sync,
{
    type Ok = Ok;
    type Err = Err;

    fn run<'a>(self) -> BoxFuture<'a, Result<Self::Ok, (Self::Err, Self)>> where Self: 'a {
        Box::pin(async move {
            let result = (self.0)(self.1).await;
            match result {
                Ok(ok) => Ok(ok),
                Err((err, args)) => Err((err, (self.0, args))),
            }
        })
    }
}

/// An algorithm loosely based off of TCP Tahoe for dynamically configuring concurrency
/// limits in the face of errors.
pub struct Tahoe {
    ssthresh: usize,
    aimd_counter: usize,
    mss: usize,
}

impl Default for Tahoe {
    fn default() -> Self {
        Self {
            ssthresh: usize::MAX,
            aimd_counter: 0,
            mss: 10,
        }
    }
}

/// An algorithm that can be used to adjust the concurrency limits of the Stream.
pub trait Algorithm: Unpin {
    /// The logic to run when a Future successfully returns an Ok.
    ///
    /// # Parameters
    /// current_size: the number of currently in flight Futures + this one.
    /// desired_size: the most recent desired_size decided by this algorithm.
    ///
    /// # Return
    ///
    /// The new desired_size
    fn on_ok(&mut self, current_size: usize, desired_size: usize) -> usize;

    /// The logic to run when a Future fails and returns an Err.
    ///
    /// # Parameters
    /// current_size: the number of currently in flight Futures + this one.
    /// desired_size: the most recent desired_size decided by this algorithm.
    ///
    /// # Return
    ///
    /// The new desired_size
    fn on_err(&mut self, current_size: usize, desired_size: usize) -> usize;

    /// The starting desire_size of the Stream.
    fn starting_size(&self) -> usize;
}

impl Algorithm for Tahoe {
    fn on_ok(&mut self, current_size: usize, desired_size: usize) -> usize {
        if current_size != desired_size {
            return desired_size;
        }
        if current_size < self.ssthresh {
            self.aimd_counter = 0;
            return desired_size + 1;
        } else {
            self.aimd_counter += 1;
            if self.aimd_counter >= current_size {
                self.aimd_counter = 0;
                return desired_size + 1;
            }
        }
        desired_size
    }

    fn on_err(&mut self, current_size: usize, desired_size: usize) -> usize {
        if current_size > desired_size {
            return desired_size;
        }
        self.aimd_counter = 0;
        self.ssthresh = current_size / 2;
        if self.ssthresh < self.mss {
            self.mss = self.ssthresh;
            if self.mss == 0 {
                self.mss = 1;
            }
        }
        self.mss
    }

    fn starting_size(&self) -> usize {
        self.mss
    }
}

type PendingFutures<R> = FuturesUnordered<
            BoxFuture<'static, Result<<R as Retryable>::Ok, (<R as Retryable>::Err, R)>>,
        >;

/// A Stream that dynamically manages concurrency and runs fallible Futures until they *all succeed*
pub struct AutoBuffer<R, A = Tahoe>
where
    R: Retryable,
{
    algorithm: A,
    coroutines: VecDeque<R>,
    pending: PendingFutures<R>,
    desired_size: usize,
}

impl<R, A> AutoBuffer<R, A>
where
    R: Retryable,
{
    /// Add a new Retryable Future generator to the end of the Stream
    pub fn push(&mut self, retryable: R) {
        self.coroutines.push_back(retryable);
    }

    /// Add a new Retryable Future generator to the start of the Stream
    fn push_front(&mut self, retryable: R) {
        self.coroutines.push_front(retryable);
    }

    /// Add a new collection of Retryable Future generator to the end of the Stream
    pub fn extend<I: IntoIterator<Item = R>>(&mut self, iterator: I) {
        self.coroutines.extend(iterator);
    }
}

impl<R, A> AutoBuffer<R, A>
where
    A: Default + Algorithm,
    R: Retryable,
{
    /// Construct a new AutoBuffer (same as AutoBuffer::default())
    pub fn new() -> Self {
        Self::default()
    }
}

impl<R, A> Default for AutoBuffer<R, A>
where
    A: Default + Algorithm,
    R: Retryable,
{
    fn default() -> Self {
        Self::with_algorithm(A::default())
    }
}

impl<R, A: Algorithm> AutoBuffer<R, A>
where
    A: Algorithm,
    R: Retryable,
{
    /// Construct a new AutoBuffer with a pre-initialized Algorithm
    pub fn with_algorithm(algorithm: A) -> Self {
        let desired_size = algorithm.starting_size();
        Self {
            algorithm,
            coroutines: VecDeque::default(),
            pending: FuturesUnordered::default(),
            desired_size,
        }
    }
}

impl<R, A: Algorithm + Default, I> From<I> for AutoBuffer<R, A>
where
    I: IntoIterator<Item = R>,
    R: Retryable,
{
    fn from(iterator: I) -> Self {
        let mut buffer = Self::default();
        buffer.extend(iterator);
        buffer
    }
}

impl<R, A> AutoBuffer<R, A>
where
    R: Retryable + Send + Sync + 'static,
{
    // First {desired_size} are executing
    fn start_futures(&mut self) {
        let length = self.pending.len();
        if self.desired_size > length {
            // start more
            for _ in length..self.desired_size {
                if let Some(coroutine) = self.coroutines.pop_front() {
                    self.pending.push(coroutine.run());
                } else {
                    return;
                }
            }
        }
    }
}

impl<R, A> Stream for AutoBuffer<R, A>
where
    R: Retryable + Send + Sync + 'static,
    A: Algorithm,
{
    type Item = R::Ok;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // get next completed future
        // dispatch to on_ok or on_err
        // check cwnd vs. self.pending to see if more should be started
        loop {
            self.start_futures();
            let length = self.pending.len();
            match self.pending.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(value))) => {
                    let desired_size = self.desired_size;
                    self.desired_size = self.algorithm.on_ok(length, desired_size);
                    return Poll::Ready(Some(value));
                }
                Poll::Ready(Some(Err((_, retryable)))) => {
                    let desired_size = self.desired_size;
                    self.desired_size = self.algorithm.on_err(length, desired_size);
                    self.push_front(retryable);
                    continue;
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tahoe {
    use super::*;

    #[test]
    fn slow_start_ok() {
        let mut tahoe = Tahoe::default();
        assert_eq!(tahoe.on_ok(9, 10), 10);
        assert_eq!(tahoe.on_ok(10, 10), 11);
        assert_eq!(tahoe.on_ok(11, 10), 10);
    }

    #[test]
    fn slow_start_err() {
        let mut tahoe = Tahoe::default();
        assert_eq!(tahoe.on_err(100, 100), 10);
        let mut tahoe = Tahoe::default();
        assert_eq!(tahoe.on_err(9, 10), 4);
        let mut tahoe = Tahoe::default();
        assert_eq!(tahoe.on_err(10, 10), 5);
        let mut tahoe = Tahoe::default();
        assert_eq!(tahoe.on_err(11, 10), 10);
    }

    #[test]
    fn slow_start_transition() {
        let mut tahoe = Tahoe::default();
        tahoe.ssthresh = 11;
        assert_eq!(tahoe.on_ok(10, 10), 11);
        assert_eq!(tahoe.on_ok(11, 11), 11);
    }

    #[test]
    fn aimd_ok() {
        let mut tahoe = Tahoe::default();
        tahoe.ssthresh = 0;
        assert_eq!(tahoe.on_ok(3, 3), 3);
        assert_eq!(tahoe.on_ok(3, 3), 3);
        assert_eq!(tahoe.on_ok(3, 3), 4);
        assert_eq!(tahoe.on_ok(4, 4), 4);
    }

    #[test]
    fn aimd_err() {
        let mut tahoe = Tahoe::default();
        tahoe.ssthresh = 99;
        assert_eq!(tahoe.on_err(100, 100), 10);
        let mut tahoe = Tahoe::default();
        tahoe.ssthresh = 99;
        assert_eq!(tahoe.on_err(102, 101), 101);
        let mut tahoe = Tahoe::default();
        tahoe.ssthresh = 99;
        assert_eq!(tahoe.on_err(100, 101), 10);
        let mut tahoe = Tahoe::default();
        tahoe.ssthresh = 10;
        assert_eq!(tahoe.on_err(10, 10), 5);
    }

    #[test]
    fn back_to_slow_start() {
        let mut tahoe = Tahoe::default();
        tahoe.ssthresh = 100;
        assert_eq!(tahoe.on_err(101, 101), 10);
        assert_eq!(tahoe.on_ok(10, 10), 11);
    }
}

#[cfg(test)]
mod auto_buffer {
    use super::*;
    use futures::stream::StreamExt;
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::Arc;
    use tokio::task::yield_now;

    #[tokio::test]
    async fn empty_function() {
        async fn foo() -> Result<(), ()> {
            Ok(())
        }

        let generators = vec![foo; 1000];
        let buffer = AutoBuffer::<_, Tahoe>::from(generators);
        let results: Vec<()> = buffer.collect().await;
        assert_eq!(results.len(), 1000);
    }

    #[tokio::test]
    async fn with_arguments() {
        struct Client;

        impl Client {
            async fn request(&self, _params: &str) -> Result<(), ()> {
                yield_now().await;
                Ok(())
            }
        }

        async fn my_generator(
            args: (Arc<Client>, String),
        ) -> Result<(), ((), (Arc<Client>, String))> {
            let result = args.0.request(&args.1).await;
            match result {
                Ok(()) => Ok(()),
                Err(e) => Err((e, args)),
            }
        }

        let client = Arc::new(Client);
        let generators: Vec<_> = (0u32..100)
            .map(|i| {
                let client = client.clone();
                let params = i.to_string();
                (my_generator, (client, params))
            })
            .collect();
        let buffer = AutoBuffer::<_, Tahoe>::from(generators);
        let results: Vec<()> = buffer.collect().await;
        assert_eq!(results.len(), 100);
    }

    #[tokio::test]
    async fn with_errors() {
        async fn can_fail(input: AtomicU8) -> Result<(), ((), AtomicU8)> {
            if input.fetch_sub(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(((), input))
            }
        }

        let generators: Vec<_> = (1u8..101)
            .map(|i| {
                let input = if i % 16 == 0 { i / 16 } else { 0 };
                (can_fail, AtomicU8::new(input))
            })
            .collect();
        let buffer = AutoBuffer::<_, Tahoe>::from(generators);
        let results: Vec<()> = buffer.collect().await;
        assert_eq!(results.len(), 100);
    }

    #[tokio::test]
    async fn custom_algorithm() {
        struct OnceOnly;

        impl Algorithm for OnceOnly {
            fn on_ok(&mut self, _: usize, _: usize) -> usize {
                1
            }

            fn on_err(&mut self, _: usize, _: usize) -> usize {
                1
            }

            fn starting_size(&self) -> usize {
                1
            }
        }

        async fn concurrency_detector(input: Arc<AtomicU8>) -> Result<(), ((), Arc<AtomicU8>)> {
            assert_eq!(input.fetch_add(1, Ordering::SeqCst), 0);
            yield_now().await;
            assert_eq!(input.fetch_sub(1, Ordering::SeqCst), 1);
            Ok(())
        }

        let input = Arc::new(AtomicU8::new(0));
        let generators: Vec<_> = (1u8..101)
            .map(|_| (concurrency_detector, Arc::clone(&input)))
            .collect();
        let mut buffer = AutoBuffer::with_algorithm(OnceOnly);
        buffer.extend(generators);
        let results: Vec<()> = buffer.collect().await;
        assert_eq!(results.len(), 100);
    }

    #[tokio::test]
    async fn desired_size() {
        async fn can_fail(input: AtomicU8) -> Result<(), ((), AtomicU8)> {
            if input.fetch_sub(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(((), input))
            }
        }

        let generators: Vec<_> = (0u8..11)
            .map(|i| {
                let input = if i == 10 { 1 } else { 0 };
                (can_fail, AtomicU8::new(input))
            })
            .collect();
        let mut buffer = AutoBuffer::<_, Tahoe>::from(generators);
        assert!(buffer.next().await.is_some());
        assert_eq!(buffer.desired_size, 11);
        assert_eq!(buffer.pending.len(), 9);
    }
}
