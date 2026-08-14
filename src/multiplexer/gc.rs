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

    for (key, sender) in sessions {
        let (reply_tx, reply_rx) = oneshot::channel();
        if sender
            .send(ActorCommand::EvictIfIdle {
                idle_timeout: multiplexer.idle_timeout(),
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            multiplexer
                .sessions
                .remove_if(&key, |_, current| current.same_channel(&sender));
            continue;
        }

        match reply_rx.await {
            Ok(Ok(true)) => {
                multiplexer
                    .sessions
                    .remove_if(&key, |_, current| current.same_channel(&sender));
                evicted += 1;
            }
            Ok(Ok(false)) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                multiplexer
                    .sessions
                    .remove_if(&key, |_, current| current.same_channel(&sender));
            }
        }
    }
    Ok(evicted)
}
