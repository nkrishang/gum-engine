//! Group commit for job ingest.
//!
//! Every accepted request must be durable before the API answers 202, but one INSERT round trip per
//! request would cap ingest at a few hundred per second per connection. Handlers hand their job to this
//! batcher instead and wait; the batcher writes everything that arrived in the meantime as one statement
//! and acknowledges each caller individually. Under light load a job is written immediately (no added
//! latency); under heavy load batches grow naturally while the previous write is in flight.

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::{
    jobs::{InsertOutcome, NewJob},
    Store,
};
use crate::error::StoreError;

const MAX_BATCH: usize = 500;
/// Upper bound on how long the first job of a batch waits for company.
const LINGER: Duration = Duration::from_millis(2);

type Ack = oneshot::Sender<Result<InsertOutcome, String>>;

#[derive(Clone)]
pub struct IngestBatcher {
    tx: mpsc::Sender<(NewJob, Ack)>,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("the store is unavailable: {0}")]
    Unavailable(String),
    #[error("ingest queue is full")]
    Overloaded,
}

impl IngestBatcher {
    pub fn spawn(store: Store) -> Self {
        let (tx, rx) = mpsc::channel(8_192);
        tokio::spawn(run(store, rx));
        Self { tx }
    }

    /// Returns once the job is committed (or definitively not).
    pub async fn submit(&self, job: NewJob) -> Result<InsertOutcome, IngestError> {
        let (ack, done) = oneshot::channel();
        self.tx.try_send((job, ack)).map_err(|_| IngestError::Overloaded)?;
        match done.await {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(e)) => Err(IngestError::Unavailable(e)),
            Err(_) => Err(IngestError::Unavailable("ingest batcher stopped".into())),
        }
    }
}

async fn run(store: Store, mut rx: mpsc::Receiver<(NewJob, Ack)>) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        // Take whatever is already waiting; linger briefly only if more is likely on its way.
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(item) => batch.push(item),
                Err(_) => break,
            }
        }
        if batch.len() > 1 && batch.len() < MAX_BATCH {
            let deadline = tokio::time::Instant::now() + LINGER;
            while batch.len() < MAX_BATCH {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(item)) => batch.push(item),
                    _ => break,
                }
            }
        }
        metrics::histogram!("gum_ingest_batch_size").record(batch.len() as f64);

        let (jobs, acks): (Vec<NewJob>, Vec<Ack>) = batch.into_iter().unzip();
        match store.insert_jobs(&jobs).await {
            Ok(outcomes) => {
                for (ack, outcome) in acks.into_iter().zip(outcomes) {
                    let _ = ack.send(Ok(outcome));
                }
            }
            Err(batch_err) => {
                // One poisoned row must not fail its neighbours: fall back to row-by-row.
                tracing::warn!(event = "ingest.batch_failed", code = batch_err.code(), error = %batch_err, size = jobs.len(), "batched insert failed; retrying rows individually");
                for (job, ack) in jobs.into_iter().zip(acks) {
                    let result: Result<InsertOutcome, StoreError> = store.insert_jobs(std::slice::from_ref(&job)).await.map(|mut o| o.remove(0));
                    let _ = ack.send(result.map_err(|e| e.to_string()));
                }
            }
        }
    }
}
