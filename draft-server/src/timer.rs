use std::{time::Duration, ops::Range};

use futures::{future::{abortable, AbortHandle}, select};
use rand::Rng;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel, UnboundedSender};
use std::time::Instant;
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

#[derive(Debug)]
pub struct RaftTimer {
    /// The receive end of the channel that will instruct the internal timer
    /// to be reset.
    pub reset_timer_rx: UnboundedReceiver<()>,
    /// The sender end of the channel that a message will be sent to
    /// when the timer expires.
    pub timer_complete_tx: UnboundedSender<()>,

    /// A range (in milliseconds) to draw a random timeout from.
    pub duration_range: Range<u64>
}

impl RaftTimer {

    /// Create a new instance of the timer.
    /// 
    /// ## Examples
    /// 
    /// ```
    /// use draft_server::RaftTimer;
    /// use tokio::sync::mpsc::unbounded_channel;
    /// 
    /// // Our timer will randomly draw a time unit between
    /// // 50ms and 100ms.
    /// let duration_between_50ms_and_100ms = 50..100;
    /// 
    /// let (reset_tx, reset_rx) = unbounded_channel();
    /// let (timeout_tx, timeout_rx) = unbounded_channel();
    /// 
    /// let timer = RaftTimer::new(
    ///     reset_rx,
    ///     timeout_tx,
    ///     duration_between_50ms_and_100ms
    /// );
    /// ```
    pub fn new(
        reset_rx: UnboundedReceiver<()>, 
        on_complete_tx: UnboundedSender<()>, 
        duration_range: Range<u64>
    ) -> Self {
        Self {
            reset_timer_rx: reset_rx,
            timer_complete_tx: on_complete_tx,
            duration_range
        }
    }

    /// The async loop of the internal timer that will act on the
    /// "reset" messages received on the reset_rx channel, and
    /// when the timeout expires, will send "complete"
    /// messages to the on_complete channel.
    pub async fn run(self) -> color_eyre::Result<()> {
        let mut token: Option<TimeoutToken> = None;
        let mut reset_timer_rx = self.reset_timer_rx;

        while let Some(_) = reset_timer_rx.recv().await {
            trace!("Received reset.");
            if let Some(timeout_token) = token.as_ref() {
                trace!("Clearing timeout set for {:#?} ms", timeout_token.duration.as_millis());
                timeout_token.handle.abort();
            }
            let duration = get_random_duration(self.duration_range.clone());
            trace!("Resetting the timer for {:#?} ms", duration.as_millis());
            token = Some(set_timeout(self.timer_complete_tx.clone(), duration));
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::utils::set_up_logging;
    use tracing::Level;

    #[tokio::test]
    pub async fn it_works() -> color_eyre::Result<()> {
        set_up_logging(Level::TRACE);

        let (reset_tx, reset_rx) = unbounded_channel();
        let (on_complete_tx, mut on_complete_rx) = unbounded_channel();
        let duration_range: Range<u64> = 1500..3000;
        let timer = RaftTimer::new(reset_rx, on_complete_tx, duration_range.clone());
        let start_time = Instant::now();

        let initial_delay = Duration::from_secs(1);


        let t1 = tokio::spawn(async move {
            timer.run().await
        });

        let t2 = tokio::spawn(async move {
            while let Some(_) = on_complete_rx.recv().await {
                // Since the timer's may expire only after 1500ms,
                // and we reset the timer after 1s,
                // we expect the the timer to not have been completed 
                // before (1s + 1s) + (1500ms).

                // First time we receive this, must be after at least
                // (delay: 1s) + (delay: 1s) + (min_timeout: 1500ms) = 2500ms.

                let at_least_duration = 2 * initial_delay + Duration::from_millis(duration_range.clone().start);
                assert!(start_time.elapsed() >= at_least_duration);
            }
        });

        let t3 = tokio::spawn( async move {
            // Wait for `initial_delay` before starting the timer for the first time.
            tokio::time::sleep(initial_delay).await;
            reset_tx.send(()).unwrap();
            // Wait for `initial_delay` before resetting the timer.
            tokio::time::sleep(initial_delay).await;
            reset_tx.send(()).unwrap();
        });

        let _ = tokio::join!(t1, t2, t3);

        Ok(())
    }
}
