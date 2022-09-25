use std::{time::Duration, ops::Range};

use futures::{future::{abortable, AbortHandle}};
use rand::Rng;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::select;
use tracing::trace;


#[derive(Debug)]
pub struct TimeoutToken {
    handle: AbortHandle,
    duration: Duration
}


#[inline(always)]
pub fn get_random_duration(duration_range: Range<u64>) -> std::time::Duration {
    let mut rng = rand::thread_rng();
    Duration::from_millis(rng.gen_range(duration_range))
}


pub fn set_timeout(signal_tx: UnboundedSender<()>, duration: Duration) -> TimeoutToken
{
    let (abortable, abort_handle) = abortable(async move {
        tokio::time::sleep(duration).await;
        signal_tx.send(()).unwrap();
    });

    tokio::spawn(abortable);
    TimeoutToken { handle: abort_handle, duration }
}


pub fn set_interval(signal_tx: UnboundedSender<()>, duration: Duration, immediate: bool) -> TimeoutToken
{
    let (abortable, abort_handle) = abortable(async move {
        let mut interval = tokio::time::interval(duration);

        interval.tick().await;

        if immediate {
            signal_tx.send(()).unwrap();
        }

        loop {
            interval.tick().await;
            signal_tx.send(()).unwrap();
        }
    });
    
    tokio::spawn(abortable);
    TimeoutToken { handle: abort_handle, duration }
}

/// A helper struct to wrap the functionality offered by an abortable set_timeout.
/// A range of `u64`s (representing milliseconds) can be provided and the timeout 
/// will be set for a random duration drawn from the given range. 
/// 
/// If and when the timer elapses, a message will be sent on
/// the `timer_complete_tx` channel. Meanwhile, if a message is received 
/// on the `reset_timer_rx` channel, the existing timer will be reset to 
/// some other random duration.
#[derive(Debug)]
pub struct RaftTimeout {
    /// The receive end of the channel that will instruct the internal timer
    /// to be reset.
    pub reset_timer_rx: UnboundedReceiver<Duration>,
    /// The receive end of the channel that will stop the internal timer.
    pub stop_timer_rx: UnboundedReceiver<()>,
    /// The sender end of the channel that a message will be sent to
    /// when the timer expires.
    pub timer_complete_tx: UnboundedSender<()>,
}

impl RaftTimeout {

    /// Create a new instance of the timer.
    /// 
    /// ## Examples
    /// 
    /// ```
    /// use draft_server::RaftTimeout;
    /// use tokio::sync::mpsc::unbounded_channel;
    /// use std::time::Duration;
    /// 
    /// let (reset_tx, reset_rx) = unbounded_channel();
    /// let (stop_tx, stop_rx) = unbounded_channel();
    /// let (timeout_tx, timeout_rx) = unbounded_channel();
    /// 
    /// let timer = RaftTimeout::new(
    ///     reset_rx,
    ///     stop_rx,
    ///     timeout_tx,
    /// );
    /// ```
    pub fn new(
        reset_timer_rx: UnboundedReceiver<Duration>, 
        stop_timer_rx: UnboundedReceiver<()>,
        on_complete_tx: UnboundedSender<()>, 
    ) -> Self {
        Self {
            reset_timer_rx,
            stop_timer_rx,
            timer_complete_tx: on_complete_tx,
        }
    }

    /// The async loop of the internal timer that will act on the
    /// "reset" messages received on the reset_rx channel, and
    /// when the timeout expires, will send "complete"
    /// messages to the on_complete channel.
    pub async fn run(self) -> color_eyre::Result<()> {
        let mut token: Option<TimeoutToken> = None;
        let mut stop_timer_rx = self.stop_timer_rx;
        let mut reset_timer_rx = self.reset_timer_rx;

        loop {
            select! {
                Some(duration) = reset_timer_rx.recv() => {
                    if let Some(timeout_token) = token.as_ref() {
                        timeout_token.handle.abort();
                        trace!("Resetting timeout.");
                    }
                    token = Some(set_timeout(self.timer_complete_tx.clone(), duration));
                },
                Some(_) = stop_timer_rx.recv() => {
                    if let Some(timeout_token) = token.as_ref() {
                        trace!("clearing timeout set for {:#?} ms", timeout_token.duration.as_millis());
                        timeout_token.handle.abort();
                    }
                    token = None
                },
                else => break
            }
        }
        Ok(())
    }
}


/// A helper struct to wrap the functionality offered by an abortable set_interval.
/// A fixed duration can be provided so that the timer can notify after every tick of
/// that duration.
/// 
/// If and when the timer ticks, a message will be sent on
/// the `timer_complete_tx` channel. Meanwhile, if a message is received 
/// on the `reset_timer_rx` channel, the existing timer will be reset.
///
/// If `immediate` is set, the `timer_complete_tx` channel will be sent a message
/// right away, as well as after a tick of every `duration` milliseconds. Otherwise,
/// the first message is sent after at least `duration` milliseconds and periodically
/// after that.
/// 
/// If a message is received on the `stop_rx` channel, the existing timer will be stopped
/// and no more messages will be sent on the `timer_complete_tx` channel.
#[derive(Debug)]
pub struct RaftInterval {
    /// The receive end of the channel that will instruct the interval timer
    /// to be reset.
    pub restart_rx: UnboundedReceiver<()>,
    /// The receive end of the channel that will instruct the internal timer
    /// to be stopped.
    pub stop_rx: UnboundedReceiver<()>,
    /// The sender end of the channel that a message will be sent to
    /// when the interval elapses.
    pub timer_complete_tx: UnboundedSender<()>,

    /// A range (in milliseconds) to draw a random timeout from.
    pub interval_duration: Duration,
    /// A flag, if set, will send a message at the 0th tick of the interval.
    /// Otherwise, will wait for a non-zero duration of time to elapse before
    /// sending the first message.
    pub immediate: bool
}

impl RaftInterval {

    /// Create a new instance of the timer.
    /// 
    /// ## Examples
    /// 
    /// ```
    /// use draft_server::RaftInterval;
    /// use tokio::sync::mpsc::unbounded_channel;
    /// use std::time::Duration;
    /// 
    /// // Our timer will tick every 800ms.
    /// let duration = Duration::from_millis(800);
    /// 
    /// let (restart_tx, restart_rx) = unbounded_channel();
    /// let (stop_tx, stop_rx) = unbounded_channel();
    /// 
    /// let (on_tick_tx, on_tick_rx) = unbounded_channel();
    /// 
    /// let timer = RaftInterval::new(
    ///     restart_rx,
    ///     stop_rx,
    ///     on_tick_tx,
    ///     duration,
    ///     true
    /// );
    /// ```
    pub fn new(
        restart_rx: UnboundedReceiver<()>,
        stop_rx: UnboundedReceiver<()>, 
        on_complete_tx: UnboundedSender<()>, 
        interval_duration: Duration,
        immediate: bool,
    ) -> Self {
        Self {
            restart_rx,
            stop_rx,
            timer_complete_tx: on_complete_tx,
            interval_duration,
            immediate
        }
    }

    /// Start the async event loop of the internal timer that will act on the
    /// "restart" messages received on the `reset_rx` channel, and "stop"
    /// messages received on the `stop_rx` channel and on every tick of the timer
    /// will send "complete" messages to the on_complete channel.
    pub async fn run(self) -> color_eyre::Result<()> {

        let mut token: Option<TimeoutToken> = None;
        let mut restart_rx = self.restart_rx;
        let mut stop_rx = self.stop_rx;
        let on_complete_tx = self.timer_complete_tx.clone();
        let duration = self.interval_duration.clone();

        loop {
            select! {
                Some(_) = restart_rx.recv() => {
                    
                    if let Some(timeout_token) = token.as_ref() {
                        trace!("clearing interval set for {:#?} ms", timeout_token.duration.as_millis());
                        timeout_token.handle.abort();
                    }

                    token = Some(set_interval(on_complete_tx.clone(), duration, self.immediate));
                    
                },
                Some(_) = stop_rx.recv() => {

                    if let Some(timeout_token) = token.as_ref() {
                        trace!("clearing interval set for {:#?} ms", timeout_token.duration.as_millis());
                        timeout_token.handle.abort();
                    }
                    token = None
                },
                else => break,
            }
        }

        Ok(())

    }

}


#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::utils::set_up_logging;
    use tracing::Level;
    use tokio::sync::mpsc::unbounded_channel;
    use std::{time::Instant, sync::atomic::{AtomicU64, Ordering}};

    #[tokio::test]
    pub async fn timeout_works() -> color_eyre::Result<()> {
        set_up_logging(Level::TRACE);

        let (reset_tx, reset_rx) = unbounded_channel();
        let (_stop_tx, stop_rx) = unbounded_channel();
        drop(_stop_tx); // We're not gonna send the stop signal.
        let (on_complete_tx, mut on_complete_rx) = unbounded_channel();
        let timer = RaftTimeout::new(reset_rx, stop_rx, on_complete_tx);
        let start_time = Instant::now();

        let t1 = tokio::spawn(async move {
            timer.run().await
        });

        let counter: AtomicU64 = AtomicU64::from(0);

        let t2 = tokio::spawn(async move {
            while let Some(_) = on_complete_rx.recv().await {
                counter.fetch_add(1, Ordering::SeqCst);
                // Since the timer's may expire only after 1500ms,
                // and we reset the timer after 1s,
                // we expect the the timer to not have been completed 
                // before (1s + 1s) + (1500ms).

                // First time we receive this, must be after at least
                // (delay: 1s) + (delay: 1s) + (min_timeout: 1500ms) = 2500ms.

                let at_least_duration = Duration::from_millis(1900);
                assert!(start_time.elapsed() >= at_least_duration);
            }
            assert_eq!(counter.load(Ordering::SeqCst), 1);
        });

        let t3 = tokio::spawn( async move {
            // Wait for `initial_delay` before starting the timer for the first time.
            // tokio::time::sleep(initial_delay).await;
            reset_tx.send(Duration::from_secs(1)).unwrap();
            // Wait for `initial_delay` before resetting the timer.
            tokio::time::sleep(Duration::from_millis(900)).await;
            reset_tx.send(Duration::from_secs(1)).unwrap();
        });


        let _ = tokio::join!(t1, t2, t3);

        Ok(())
    }

    #[tokio::test]
    pub async fn timeout_with_stop_works() -> color_eyre::Result<()> {
        set_up_logging(Level::TRACE);

        let (reset_tx, reset_rx) = unbounded_channel();
        let (stop_tx, stop_rx) = unbounded_channel();
        let (on_complete_tx, mut on_complete_rx) = unbounded_channel();
        let timer = RaftTimeout::new(reset_rx, stop_rx, on_complete_tx);
        let start_time = Instant::now();

        let t1 = tokio::spawn(async move {
            timer.run().await
        });

        let counter: AtomicU64 = AtomicU64::from(0);

        let t2 = tokio::spawn(async move {
            while let Some(_) = on_complete_rx.recv().await {
                counter.fetch_add(1, Ordering::SeqCst);
                // Since the timer's may expire only after 1500ms,
                // and we reset the timer after 1s,
                // we expect the the timer to not have been completed 
                // before (1s + 1s) + (1500ms).

                // First time we receive this, must be after at least
                // (delay: 1s) + (delay: 1s) + (min_timeout: 1500ms) = 2500ms.

                let at_least_duration = Duration::from_millis(1900);
                assert!(start_time.elapsed() >= at_least_duration);
            }
            // Because we send the stop signal right after 3500ms.
            assert_eq!(counter.load(Ordering::SeqCst), 0);
        });

        let t3 = tokio::spawn( async move {
            // Wait for `initial_delay` before starting the timer for the first time.
            reset_tx.send(Duration::from_secs(1)).unwrap();
            // Wait for `initial_delay` before resetting the timer.
            tokio::time::sleep(Duration::from_millis(900)).await;
            reset_tx.send(Duration::from_secs(1)).unwrap();
        });

        let t4 = tokio::spawn( async move {
            // Wait for 3.5s before stopping the timer.
            tokio::time::sleep(Duration::from_millis(1800)).await;
            stop_tx.send(()).unwrap();
        });

        let _ = tokio::join!(t1, t2, t3, t4);

        Ok(())
    }

    #[tokio::test]
    pub async fn interval_works() -> color_eyre::Result<()> {
        set_up_logging(Level::TRACE);

        let (restart_tx, restart_rx) = unbounded_channel();
        let (stop_tx, stop_rx) = unbounded_channel();
        let (on_complete_tx, mut on_complete_rx) = unbounded_channel();
        
        let duration = Duration::from_secs(1);
        
        let timer = RaftInterval::new(restart_rx, stop_rx, on_complete_tx, duration, true);

        let start_time = Instant::now();

        let counter: AtomicU64 = AtomicU64::from(0);


        let t1 = tokio::spawn(async move {
            timer.run().await
        });

        let t2 = tokio::spawn(async move {
            while let Some(_) = on_complete_rx.recv().await {
                trace!("On complete received.");
                let prev_counter = counter.fetch_add(1, Ordering::SeqCst);
                
                // Since the timer should tick every 1 second,
                // and we reset the timer after 2.5s,
                // we expect the timer to tick 3 times 
                // (at the start, 1-second mark, and 2-second mark).

                assert!(start_time.elapsed() >= Duration::from_secs(prev_counter));

            }

            // Since we send the stop signal at 2.5 seconds, we expect that only 3 complete signals
            // were received.
            assert_eq!(counter.load(Ordering::SeqCst), 3);
        });

        let t3 = tokio::spawn( async move {
            // Wait for 2.5s before stopping the timer.
            tokio::time::sleep(Duration::from_millis(2500)).await;
            stop_tx.send(()).unwrap();
        });

        let t4 = tokio::spawn(async move {
            // Send the start signal immediately.
            restart_tx.send(()).unwrap();
        });

        let _ = tokio::join!(t1, t2, t3, t4);
        Ok(())

    }

}
