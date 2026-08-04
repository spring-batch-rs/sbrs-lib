//! Bounded concurrency for chunk writes.
//!
//! Holds at most `limit` in-flight write tasks. Errors are surfaced when
//! harvested — on a later call, or at [`InflightWrites::drain`] — never at the
//! moment the failing write was dispatched.

use std::future::Future;

use tokio::task::JoinSet;

use crate::BatchError;
use crate::item::rdbc::writer_common::create_write_error;

/// A bounded set of in-flight database writes.
///
/// # Implementation Note
///
/// `spawn` blocks when the limit is reached; that block *is* the backpressure,
/// so no separate counter is maintained. `harvest` never blocks, because the
/// step engine calls `ItemWriter::flush` after every chunk and blocking there
/// would serialise everything again.
pub(crate) struct InflightWrites {
    limit: usize,
    set: JoinSet<Result<(), sqlx::Error>>,
    table: String,
    db_name: &'static str,
}

impl InflightWrites {
    /// Creates a set allowing at most `limit` concurrent writes.
    ///
    /// A `limit` of zero is clamped to one: a zero limit would make [`spawn`]
    /// loop forever waiting for a slot it can never get.
    ///
    /// [`spawn`]: InflightWrites::spawn
    pub(crate) fn new(limit: usize, table: String, db_name: &'static str) -> Self {
        Self {
            limit: limit.max(1),
            set: JoinSet::new(),
            table,
            db_name,
        }
    }

    /// Dispatches a write, first waiting for a slot if the limit is reached.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::ItemWriter`] if a *previously* dispatched write
    /// failed and was harvested while making room.
    pub(crate) fn spawn<F>(&mut self, fut: F) -> Result<(), BatchError>
    where
        F: Future<Output = Result<(), sqlx::Error>> + Send + 'static,
    {
        let mut first_error = self.harvest();

        while self.set.len() >= self.limit {
            let joined = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(self.set.join_next())
            });
            match joined {
                Some(outcome) => {
                    if let Err(e) = self.interpret(outcome)
                        && first_error.is_ok()
                    {
                        first_error = Err(e);
                    }
                }
                None => break,
            }
        }

        self.set.spawn(fut);
        first_error
    }

    /// Collects results of already-finished writes without waiting.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::ItemWriter`] for the first failed write collected.
    pub(crate) fn harvest(&mut self) -> Result<(), BatchError> {
        let mut first_error = Ok(());
        while let Some(outcome) = self.set.try_join_next() {
            if let Err(e) = self.interpret(outcome)
                && first_error.is_ok()
            {
                first_error = Err(e);
            }
        }
        first_error
    }

    /// Waits for every in-flight write and returns the first error found.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::ItemWriter`] for the first failed write.
    pub(crate) fn drain(&mut self) -> Result<(), BatchError> {
        let mut first_error = Ok(());
        loop {
            let joined = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(self.set.join_next())
            });
            match joined {
                Some(outcome) => {
                    if let Err(e) = self.interpret(outcome)
                        && first_error.is_ok()
                    {
                        first_error = Err(e);
                    }
                }
                None => break,
            }
        }
        first_error
    }

    /// Turns a join outcome into a batch-level result.
    fn interpret(
        &self,
        outcome: Result<Result<(), sqlx::Error>, tokio::task::JoinError>,
    ) -> Result<(), BatchError> {
        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(create_write_error(&self.table, self.db_name, e)),
            Err(join_error) => Err(create_write_error(&self.table, self.db_name, join_error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn ok_after(
        ms: u64,
    ) -> impl std::future::Future<Output = Result<(), sqlx::Error>> + Send + 'static {
        async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Ok(())
        }
    }

    fn fail_after(
        ms: u64,
    ) -> impl std::future::Future<Output = Result<(), sqlx::Error>> + Send + 'static {
        async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Err(sqlx::Error::PoolClosed)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_overlap_writes_up_to_the_limit() {
        let mut inflight = InflightWrites::new(4, "t".to_string(), "TestDb");

        let start = Instant::now();
        for _ in 0..4 {
            inflight.spawn(ok_after(100)).unwrap();
        }
        inflight.drain().unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(300),
            "4 x 100ms writes with limit 4 should overlap, took {:?}",
            elapsed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_block_when_the_limit_is_reached() {
        let mut inflight = InflightWrites::new(2, "t".to_string(), "TestDb");

        let start = Instant::now();
        for _ in 0..4 {
            inflight.spawn(ok_after(100)).unwrap();
        }
        inflight.drain().unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(200),
            "4 x 100ms writes with limit 2 must serialise into 2 waves, took {:?}",
            elapsed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_surface_error_on_drain() {
        let mut inflight = InflightWrites::new(4, "orders".to_string(), "TestDb");

        inflight.spawn(ok_after(10)).unwrap();
        inflight.spawn(fail_after(10)).unwrap();

        let result = inflight.drain();

        assert!(result.is_err(), "drain must surface a failed write");
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("TestDb"),
            "error should name the database, got: {message}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_report_no_error_when_nothing_was_spawned() {
        let mut inflight = InflightWrites::new(4, "t".to_string(), "TestDb");

        assert!(inflight.harvest().is_ok());
        assert!(inflight.drain().is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_not_block_on_harvest() {
        let mut inflight = InflightWrites::new(4, "t".to_string(), "TestDb");
        inflight.spawn(ok_after(500)).unwrap();

        let start = Instant::now();
        let result = inflight.harvest();
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert!(
            elapsed < Duration::from_millis(50),
            "harvest must not wait for in-flight tasks, took {:?}",
            elapsed
        );

        inflight.drain().unwrap();
    }
}
