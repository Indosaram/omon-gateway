use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

use crate::Result;

use super::actor::ActorCommand;
use super::router::SessionMultiplexer;

pub struct ScaleToZero {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ScaleToZero {
    pub fn start(multiplexer: SessionMultiplexer) -> Self {
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(multiplexer.gc_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(error) = collect(&multiplexer).await {
                            tracing::error!(%error, "multiplexer garbage collection failed");
                        }
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
        Self { shutdown, task }
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

pub(crate) async fn collect(multiplexer: &SessionMultiplexer) -> Result<usize> {
    let sessions: Vec<_> = multiplexer
        .sessions
        .iter()
        .map(|entry| (entry.key().clone(), entry.value().clone()))
        .collect();
    let mut evicted = 0;

    for (key, handle) in sessions {
        if !handle.try_retire() {
            continue;
        }

        // Once retirement starts, new routes wait instead of enqueueing to this
        // actor. Waiting for already-started sends makes the eviction command a
        // strict mailbox barrier: every accepted event is queued before it.
        handle.wait_for_in_flight().await;

        let (reply_tx, reply_rx) = oneshot::channel();
        if handle
            .sender
            .send(ActorCommand::EvictIfIdle {
                idle_timeout: multiplexer.idle_timeout(),
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            multiplexer.remove_handle(&key, &handle);
            handle.mark_finished();
            continue;
        }

        match reply_rx.await {
            Ok(Ok(true)) => {
                multiplexer.remove_handle(&key, &handle);
                handle.mark_finished();
                evicted += 1;
            }
            Ok(Ok(false)) => handle.resume(),
            Ok(Err(error)) => {
                handle.resume();
                return Err(error);
            }
            Err(_) => {
                multiplexer.remove_handle(&key, &handle);
                handle.mark_finished();
            }
        }
    }
    Ok(evicted)
}
