//! # Step Module
//!
//! This module provides the core step execution functionality for the Spring Batch framework.
//! A step represents a single phase of a batch job that processes data in chunks or executes
//! a single task (tasklet).
//!
//! ## Overview
//!
//! The step module supports two main execution patterns:
//!
//! ### Chunk-Oriented Processing
//! Processes data in configurable chunks using the read-process-write pattern:
//! - **Reader**: Reads items from a data source
//! - **Processor**: Transforms items (optional)
//! - **Writer**: Writes processed items to a destination
//!
//! ### Tasklet Processing
//! Executes a single task or operation that doesn't follow the chunk pattern.
//!
//! ## Key Features
//!
//! - **Error Handling**: Configurable skip limits for fault tolerance
//! - **Metrics Tracking**: Comprehensive execution statistics
//! - **Lifecycle Management**: Proper resource management with open/close operations
//! - **Builder Pattern**: Fluent API for step configuration
//!
//! ## Examples
//!
//! ### Basic Chunk-Oriented Step
//!
//! ```rust
//! use spring_batch_rs::core::step::{StepBuilder, StepExecution, Step};
//! use spring_batch_rs::core::item::{ItemReader, ItemProcessor, ItemWriter};
//! use spring_batch_rs::BatchError;
//!
//! // Implement your reader, processor, and writer
//! # struct MyReader;
//! # impl ItemReader<String> for MyReader {
//! #     fn read(&self) -> Result<Option<String>, BatchError> { Ok(None) }
//! # }
//! # struct MyProcessor;
//! # impl ItemProcessor<String, String> for MyProcessor {
//! #     fn process(&self, item: String) -> Result<Option<String>, BatchError> { Ok(Some(item)) }
//! # }
//! # struct MyWriter;
//! # impl ItemWriter<String> for MyWriter {
//! #     fn write(&self, items: &[String]) -> Result<(), BatchError> { Ok(()) }
//! #     fn flush(&self) -> Result<(), BatchError> { Ok(()) }
//! #     fn open(&self) -> Result<(), BatchError> { Ok(()) }
//! #     fn close(&self) -> Result<(), BatchError> { Ok(()) }
//! # }
//!
//! let reader = MyReader;
//! let processor = MyProcessor;
//! let writer = MyWriter;
//!
//! let step = StepBuilder::new("my-step")
//!     .chunk(100)                    // Process 100 items per chunk
//!     .reader(&reader)
//!     .processor(&processor)
//!     .writer(&writer)
//!     .skip_limit(10)               // Allow up to 10 errors
//!     .build();
//!
//! let mut step_execution = StepExecution::new(step.get_name());
//! let result = step.execute(&mut step_execution);
//! ```
//!
//! ### Tasklet Step
//!
//! ```rust
//! use spring_batch_rs::core::step::{StepBuilder, StepExecution, RepeatStatus, Step, Tasklet};
//! use spring_batch_rs::BatchError;
//!
//! # struct MyTasklet;
//! # impl Tasklet for MyTasklet {
//! #     fn execute(&self, _step_execution: &StepExecution) -> Result<RepeatStatus, BatchError> {
//! #         Ok(RepeatStatus::Finished)
//! #     }
//! # }
//!
//! let tasklet = MyTasklet;
//!
//! let step = StepBuilder::new("my-tasklet-step")
//!     .tasklet(&tasklet)
//!     .build();
//!
//! let mut step_execution = StepExecution::new(step.get_name());
//! let result = step.execute(&mut step_execution);
//! ```

use crate::BatchError;
use log::{debug, error, info, warn};
use std::time::{Duration, Instant};
use uuid::Uuid;

use super::item::{ItemProcessor, ItemReader, ItemWriter};

/// A tasklet represents a single task or operation that can be executed as part of a step.
///
/// Tasklets are useful for operations that don't fit the chunk-oriented processing model,
/// such as file operations, database maintenance, or custom business logic.
///
/// # Examples
///
/// ```rust
/// use spring_batch_rs::core::step::{StepExecution, RepeatStatus};
/// use spring_batch_rs::BatchError;
///
/// use spring_batch_rs::core::step::Tasklet;
///
/// struct FileCleanupTasklet {
///     directory: String,
/// }
///
/// impl Tasklet for FileCleanupTasklet {
///     fn execute(&self, _step_execution: &StepExecution) -> Result<RepeatStatus, BatchError> {
///         // Perform file cleanup logic here
///         println!("Cleaning up directory: {}", self.directory);
///         Ok(RepeatStatus::Finished)
///     }
/// }
/// ```
pub trait Tasklet {
    /// Executes the tasklet operation.
    ///
    /// # Parameters
    /// - `step_execution`: The current step execution context for accessing metrics and state
    ///
    /// # Returns
    /// - `Ok(RepeatStatus)`: The tasklet completed successfully
    /// - `Err(BatchError)`: An error occurred during execution
    fn execute(&self, step_execution: &StepExecution) -> Result<RepeatStatus, BatchError>;
}

/// A step implementation that executes a single tasklet.
///
/// TaskletStep is used for operations that don't follow the chunk-oriented processing pattern.
/// It executes a single tasklet and manages the step lifecycle.
///
/// # Examples
///
/// ```rust
/// use spring_batch_rs::core::step::{StepBuilder, StepExecution, RepeatStatus, Tasklet};
/// use spring_batch_rs::BatchError;
///
/// # struct MyTasklet;
/// # impl Tasklet for MyTasklet {
/// #     fn execute(&self, _step_execution: &StepExecution) -> Result<RepeatStatus, BatchError> {
/// #         Ok(RepeatStatus::Finished)
/// #     }
/// # }
/// let tasklet = MyTasklet;
/// let step = StepBuilder::new("tasklet-step")
///     .tasklet(&tasklet)
///     .build();
/// ```
pub struct TaskletStep<'a> {
    name: String,
    tasklet: &'a dyn Tasklet,
}

impl Step for TaskletStep<'_> {
    fn execute(&self, step_execution: &mut StepExecution) -> Result<(), BatchError> {
        step_execution.status = StepStatus::Started;
        let start_time = Instant::now();

        info!(
            "Start of step: {}, id: {}",
            step_execution.name, step_execution.id
        );

        loop {
            let result = self.tasklet.execute(step_execution);
            match result {
                Ok(RepeatStatus::Continuable) => {}
                Ok(RepeatStatus::Finished) => {
                    step_execution.status = StepStatus::Success;
                    break;
                }
                Err(e) => {
                    error!(
                        "Error in step: {}, id: {}, error: {}",
                        step_execution.name, step_execution.id, e
                    );
                    step_execution.status = StepStatus::Failed;
                    step_execution.end_time = Some(Instant::now());
                    step_execution.duration = Some(start_time.elapsed());
                    return Err(e);
                }
            }
        }

        // Calculate the step execution details
        step_execution.start_time = Some(start_time);
        step_execution.end_time = Some(Instant::now());
        step_execution.duration = Some(start_time.elapsed());

        Ok(())
    }

    fn get_name(&self) -> &str {
        &self.name
    }
}

/// Builder for creating TaskletStep instances.
///
/// Provides a fluent API for configuring tasklet steps with validation
/// to ensure all required components are provided.
///
/// # Examples
///
/// ```rust
/// use spring_batch_rs::core::step::{TaskletBuilder, Tasklet, RepeatStatus, StepExecution};
/// use spring_batch_rs::BatchError;
///
/// # struct MyTasklet;
/// # impl Tasklet for MyTasklet {
/// #     fn execute(&self, _step_execution: &StepExecution) -> Result<RepeatStatus, BatchError> {
/// #         Ok(RepeatStatus::Finished)
/// #     }
/// # }
///
/// let tasklet = MyTasklet;
/// let builder = TaskletBuilder::new("my-tasklet")
///     .tasklet(&tasklet);
/// let step = builder.build();
/// ```
pub struct TaskletBuilder<'a> {
    name: String,
    tasklet: Option<&'a dyn Tasklet>,
}

impl<'a> TaskletBuilder<'a> {
    /// Creates a new TaskletBuilder with the specified name.
    ///
    /// # Parameters
    /// - `name`: Human-readable name for the step
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::TaskletBuilder;
    ///
    /// let builder = TaskletBuilder::new("file-cleanup-step");
    /// ```
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            tasklet: None,
        }
    }

    /// Sets the tasklet to be executed by this step.
    ///
    /// # Parameters
    /// - `tasklet`: The tasklet implementation to execute
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::{TaskletBuilder, Tasklet, RepeatStatus, StepExecution};
    /// use spring_batch_rs::BatchError;
    ///
    /// # struct MyTasklet;
    /// # impl Tasklet for MyTasklet {
    /// #     fn execute(&self, _step_execution: &StepExecution) -> Result<RepeatStatus, BatchError> {
    /// #         Ok(RepeatStatus::Finished)
    /// #     }
    /// # }
    ///
    /// let tasklet = MyTasklet;
    /// let builder = TaskletBuilder::new("my-step")
    ///     .tasklet(&tasklet);
    /// ```
    pub fn tasklet(mut self, tasklet: &'a dyn Tasklet) -> Self {
        self.tasklet = Some(tasklet);
        self
    }

    /// Builds the TaskletStep instance.
    ///
    /// # Panics
    /// Panics if no tasklet has been set using the `tasklet()` method.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::{TaskletBuilder, Tasklet, RepeatStatus, StepExecution};
    /// use spring_batch_rs::BatchError;
    ///
    /// # struct MyTasklet;
    /// # impl Tasklet for MyTasklet {
    /// #     fn execute(&self, _step_execution: &StepExecution) -> Result<RepeatStatus, BatchError> {
    /// #         Ok(RepeatStatus::Finished)
    /// #     }
    /// # }
    ///
    /// let tasklet = MyTasklet;
    /// let step = TaskletBuilder::new("my-step")
    ///     .tasklet(&tasklet)
    ///     .build();
    /// ```
    pub fn build(self) -> TaskletStep<'a> {
        TaskletStep {
            name: self.name,
            tasklet: self
                .tasklet
                .expect("Tasklet is required for building a step"),
        }
    }
}

/// Represents the execution context and metrics for a step.
///
/// StepExecution tracks all relevant information about a step's execution,
/// including timing, item counts, error counts, and current status.
///
/// # Construction
///
/// This type is `#[non_exhaustive]`, so it cannot be built with a struct literal from
/// outside the crate — use [`StepExecution::new`]. Fields stay public and freely
/// readable; the attribute exists so that new metrics can be added in a minor release
/// rather than a breaking one.
///
/// # Examples
///
/// ```rust
/// use spring_batch_rs::core::step::{StepExecution, StepStatus};
///
/// let mut step_execution = StepExecution::new("data-processing-step");
/// assert_eq!(step_execution.status, StepStatus::Starting);
/// assert_eq!(step_execution.read_count, 0);
/// assert_eq!(step_execution.write_count, 0);
/// assert_eq!(step_execution.filter_count, 0);
/// ```
#[derive(Clone)]
#[non_exhaustive]
pub struct StepExecution {
    /// Unique identifier for this step instance
    pub id: Uuid,
    /// Human-readable name for the step
    pub name: String,
    /// Current status of the step execution
    pub status: StepStatus,
    /// Timestamp when the step started execution
    pub start_time: Option<Instant>,
    /// Timestamp when the step completed execution
    pub end_time: Option<Instant>,
    /// Total duration of step execution
    pub duration: Option<Duration>,
    /// Number of items successfully read from the source
    pub read_count: usize,
    /// Number of items successfully written to the destination
    pub write_count: usize,
    /// Number of errors encountered during reading
    pub read_error_count: usize,
    /// Number of items successfully processed and passed to the writer (excludes filtered items)
    pub process_count: usize,
    /// Number of errors encountered during processing
    pub process_error_count: usize,
    /// Number of items filtered by the processor (processor returned Ok(None))
    pub filter_count: usize,
    /// Number of errors encountered during writing
    pub write_error_count: usize,
    /// Cumulative time spent in the read phase across all chunks. Covers the whole
    /// per-chunk read loop, including the framework's buffering, not just `ItemReader::read`.
    /// Only chunk-oriented steps populate this; tasklet steps leave it at zero.
    pub read_duration: Duration,
    /// Cumulative time spent in the process phase across all chunks. Covers the whole
    /// per-chunk process loop, including collecting the results, not just `ItemProcessor::process`.
    /// Only chunk-oriented steps populate this; tasklet steps leave it at zero.
    pub process_duration: Duration,
    /// Cumulative time spent in `ItemWriter::write` across all chunks, excluding the flush
    /// that follows it. Only chunk-oriented steps populate this; tasklet steps leave it at zero.
    pub write_duration: Duration,
    /// Cumulative time spent in `ItemWriter::flush` across all chunks, measured separately
    /// from [`StepExecution::write_duration`] so the cost of flushing once per chunk can be
    /// quantified. Only chunk-oriented steps populate this; tasklet steps leave it at zero.
    pub flush_duration: Duration,
}

impl StepExecution {
    /// Creates a new StepExecution with the specified name.
    ///
    /// Initializes all counters to zero and sets the status to `Starting`.
    /// A unique UUID is generated for this execution instance.
    ///
    /// # Parameters
    /// - `name`: Human-readable name for the step
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::{StepExecution, StepStatus};
    ///
    /// let step_execution = StepExecution::new("my-step");
    /// assert_eq!(step_execution.name, "my-step");
    /// assert_eq!(step_execution.status, StepStatus::Starting);
    /// assert!(!step_execution.id.is_nil());
    /// ```
    pub fn new(name: &str) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            status: StepStatus::Starting,
            start_time: None,
            end_time: None,
            duration: None,
            read_count: 0,
            write_count: 0,
            read_error_count: 0,
            process_count: 0,
            process_error_count: 0,
            filter_count: 0,
            write_error_count: 0,
            read_duration: Duration::ZERO,
            process_duration: Duration::ZERO,
            write_duration: Duration::ZERO,
            flush_duration: Duration::ZERO,
        }
    }

    /// Formats the per-phase timing breakdown as a single human-readable line.
    ///
    /// Percentages are relative to the step's total [`StepExecution::duration`].
    /// When `duration` is `None` or zero — for instance before the step has run —
    /// every percentage is reported as `0%` rather than `NaN`.
    ///
    /// The four phases will not sum to exactly 100%. The remainder holds the writer's
    /// `open()` and `close()` calls — which is where the CSV, JSON and XML writers perform
    /// their final flush — the teardown of each processed chunk, and the framework's own
    /// bookkeeping. A large remainder is itself a useful signal.
    ///
    /// Only chunk-oriented steps populate the four phase fields. Calling this on a tasklet
    /// step's execution yields a line whose phases all read `0.0s (0%)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::StepExecution;
    /// use std::time::Duration;
    ///
    /// let mut step_execution = StepExecution::new("load-postgres");
    /// step_execution.duration = Some(Duration::from_secs(10));
    /// step_execution.read_duration = Duration::from_secs(5);
    ///
    /// let summary = step_execution.phase_summary();
    /// assert!(summary.contains("read 5.0s (50%)"));
    /// ```
    pub fn phase_summary(&self) -> String {
        let total = self.duration.unwrap_or_default().as_secs_f64();
        let pct = |d: Duration| -> f64 {
            if total > 0.0 {
                d.as_secs_f64() / total * 100.0
            } else {
                0.0
            }
        };

        format!(
            "Step '{}' {:.1}s — read {:.1}s ({:.0}%) | process {:.1}s ({:.0}%) | write {:.1}s ({:.0}%) | flush {:.1}s ({:.0}%)",
            self.name,
            total,
            self.read_duration.as_secs_f64(),
            pct(self.read_duration),
            self.process_duration.as_secs_f64(),
            pct(self.process_duration),
            self.write_duration.as_secs_f64(),
            pct(self.write_duration),
            self.flush_duration.as_secs_f64(),
            pct(self.flush_duration),
        )
    }
}

/// Represents the overall status of a batch job.
///
/// This enum defines all possible states that a batch job can be in
/// during its lifecycle, from initialization to completion.
///
/// # Examples
///
/// ```rust
/// use spring_batch_rs::core::step::BatchStatus;
///
/// let status = BatchStatus::COMPLETED;
/// match status {
///     BatchStatus::COMPLETED => println!("Job finished successfully"),
///     BatchStatus::FAILED => println!("Job failed"),
///     _ => println!("Job in progress or other state"),
/// }
/// ```
pub enum BatchStatus {
    /// The batch job has successfully completed its execution.
    COMPLETED,
    /// Status of a batch job prior to its execution.
    STARTING,
    /// Status of a batch job that is running.
    STARTED,
    /// Status of batch job waiting for a step to complete before stopping the batch job.
    STOPPING,
    /// Status of a batch job that has been stopped by request.
    STOPPED,
    /// Status of a batch job that has failed during its execution.
    FAILED,
    /// Status of a batch job that did not stop properly and can not be restarted.
    ABANDONED,
    /// Status of a batch job that is in an uncertain state.
    UNKNOWN,
}

/// Core trait that defines the contract for step execution.
///
/// All step implementations must provide execution logic and a name.
/// The step is responsible for coordinating the processing of data
/// and managing its own lifecycle.
///
/// # Examples
///
/// ```rust
/// use spring_batch_rs::core::step::{Step, StepExecution};
/// use spring_batch_rs::BatchError;
///
/// struct CustomStep {
///     name: String,
/// }
///
/// impl Step for CustomStep {
///     fn execute(&self, step_execution: &mut StepExecution) -> Result<(), BatchError> {
///         // Custom step logic here
///         Ok(())
///     }
///
///     fn get_name(&self) -> &str {
///         &self.name
///     }
/// }
/// ```
pub trait Step {
    /// Executes the step.
    ///
    /// This method represents the main operation of the step. It coordinates
    /// reading items, processing them, and writing them out for chunk-oriented
    /// steps, or executes a single task for tasklet steps.
    ///
    /// # Parameters
    /// - `step_execution`: Mutable reference to track execution state and metrics
    ///
    /// # Returns
    /// - `Ok(())`: The step completed successfully
    /// - `Err(BatchError)`: The step failed due to an error
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::{Step, StepExecution, StepStatus};
    /// use spring_batch_rs::BatchError;
    ///
    /// # struct MyStep { name: String }
    /// # impl Step for MyStep {
    /// #     fn execute(&self, step_execution: &mut StepExecution) -> Result<(), BatchError> {
    /// #         step_execution.status = StepStatus::Success;
    /// #         Ok(())
    /// #     }
    /// #     fn get_name(&self) -> &str { &self.name }
    /// # }
    /// let step = MyStep { name: "test".to_string() };
    /// let mut execution = StepExecution::new(step.get_name());
    /// let result = step.execute(&mut execution);
    /// assert!(result.is_ok());
    /// ```
    fn execute(&self, step_execution: &mut StepExecution) -> Result<(), BatchError>;

    /// Returns the name of this step.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use spring_batch_rs::core::step::{Step, StepExecution};
    /// # use spring_batch_rs::BatchError;
    /// # struct MyStep { name: String }
    /// # impl Step for MyStep {
    /// #     fn execute(&self, _step_execution: &mut StepExecution) -> Result<(), BatchError> { Ok(()) }
    /// #     fn get_name(&self) -> &str { &self.name }
    /// # }
    /// let step = MyStep { name: "data-processing".to_string() };
    /// assert_eq!(step.get_name(), "data-processing");
    /// ```
    fn get_name(&self) -> &str;
}

/// Indicates whether a tasklet should continue executing or has finished.
///
/// This enum is returned by tasklet implementations to control
/// the execution flow and indicate completion status.
///
/// # Examples
///
/// ```rust
/// use spring_batch_rs::core::step::RepeatStatus;
///
/// let status = RepeatStatus::Finished;
/// match status {
///     RepeatStatus::Continuable => println!("Tasklet can continue"),
///     RepeatStatus::Finished => println!("Tasklet has completed"),
/// }
/// ```
#[derive(Debug, PartialEq)]
pub enum RepeatStatus {
    /// The tasklet can continue to execute.
    ///
    /// This indicates that the tasklet has more work to do and should
    /// be called again in the next execution cycle.
    Continuable,
    /// The tasklet has finished executing.
    ///
    /// This indicates that the tasklet has completed all its work
    /// and should not be executed again.
    Finished,
}

/// A step implementation that processes items in chunks.
///
/// ChunkOrientedStep reads items from a reader, processes them through a processor,
/// and writes them using a writer. It handles errors gracefully with configurable
/// skip limits and provides comprehensive metrics tracking.
///
/// # Examples
///
/// ## Chunk-Oriented Processing
///
/// ```rust
/// use spring_batch_rs::core::step::{StepBuilder, StepExecution, Step};
/// use spring_batch_rs::core::item::{ItemReader, ItemProcessor, ItemWriter, PassThroughProcessor};
/// use spring_batch_rs::BatchError;
///
/// # struct MyReader;
/// # impl ItemReader<String> for MyReader {
/// #     fn read(&self) -> Result<Option<String>, BatchError> { Ok(None) }
/// # }
/// # struct MyProcessor;
/// # impl ItemProcessor<String, String> for MyProcessor {
/// #     fn process(&self, item: String) -> Result<Option<String>, BatchError> { Ok(Some(item)) }
/// # }
/// # struct MyWriter;
/// # impl ItemWriter<String> for MyWriter {
/// #     fn write(&self, items: &[String]) -> Result<(), BatchError> { Ok(()) }
/// #     fn flush(&self) -> Result<(), BatchError> { Ok(()) }
/// #     fn open(&self) -> Result<(), BatchError> { Ok(()) }
/// #     fn close(&self) -> Result<(), BatchError> { Ok(()) }
/// # }
///
/// let reader = MyReader;
/// let processor = PassThroughProcessor::<String>::new();
/// let writer = MyWriter;
///
/// let step = StepBuilder::new("my-step")
///     .chunk::<String, String>(100)                    // Process 100 items per chunk
///     .reader(&reader)
///     .processor(&processor)
///     .writer(&writer)
///     .skip_limit(10)               // Allow up to 10 errors
///     .build();
///
/// let mut step_execution = StepExecution::new(step.get_name());
/// let result = step.execute(&mut step_execution);
/// ```
pub struct ChunkOrientedStep<'a, I, O> {
    name: String,
    /// Component responsible for reading items from the source
    reader: &'a dyn ItemReader<I>,
    /// Component responsible for processing items
    processor: &'a dyn ItemProcessor<I, O>,
    /// Component responsible for writing items to the destination
    writer: &'a dyn ItemWriter<O>,
    /// Number of items to process in each chunk
    chunk_size: u16,
    /// Maximum number of errors allowed before failing the step
    skip_limit: u16,
}

impl<I, O> Step for ChunkOrientedStep<'_, I, O> {
    fn execute(&self, step_execution: &mut StepExecution) -> Result<(), BatchError> {
        // Start the timer and logging
        let start_time = Instant::now();
        info!(
            "Start of step: {}, id: {}",
            step_execution.name, step_execution.id
        );

        // Open the writer and handle any errors
        Self::manage_error(self.writer.open());

        // Main processing loop
        loop {
            // Read chunk
            let (read_items, chunk_status) = match self.read_chunk(step_execution) {
                Ok(chunk_data) => chunk_data,
                Err(_) => {
                    step_execution.status = StepStatus::ReadError;
                    break;
                }
            };

            // If no items to process, we're done
            if read_items.is_empty() {
                step_execution.status = StepStatus::Success;
                break;
            }

            // Process and write the chunk
            if self
                .process_and_write_chunk(step_execution, read_items)
                .is_err()
            {
                break; // Status already set in the method
            }

            // Check if we've reached the end
            if chunk_status == ChunkStatus::Finished {
                step_execution.status = StepStatus::Success;
                break;
            }
        }

        // Close the writer and handle any errors
        Self::manage_error(self.writer.close());

        // Log the end of the step
        info!(
            "End of step: {}, id: {}",
            step_execution.name, step_execution.id
        );

        // Calculate the step execution details
        step_execution.start_time = Some(start_time);
        step_execution.end_time = Some(Instant::now());
        step_execution.duration = Some(start_time.elapsed());

        info!("{}", step_execution.phase_summary());

        // Return the step execution details if the step is successful,
        // or an error if the step failed
        if StepStatus::Success == step_execution.status {
            Ok(())
        } else {
            Err(BatchError::Step(step_execution.name.clone()))
        }
    }

    fn get_name(&self) -> &str {
        &self.name
    }
}

impl<I, O> ChunkOrientedStep<'_, I, O> {
    /// Processes a chunk of items and writes them.
    ///
    /// This method combines the processing and writing operations for a chunk,
    /// handling errors appropriately and updating the step execution status.
    ///
    /// # Parameters
    /// - `step_execution`: Mutable reference to track execution state
    /// - `read_items`: Vector of items to process and write (consumed)
    ///
    /// # Returns
    /// - `Ok(())`: The chunk was processed and written successfully
    /// - `Err(BatchError)`: An error occurred during processing or writing
    fn process_and_write_chunk(
        &self,
        step_execution: &mut StepExecution,
        read_items: Vec<I>,
    ) -> Result<(), BatchError> {
        // Process the chunk
        let processed_items = match self.process_chunk(step_execution, read_items) {
            Ok(items) => items,
            Err(error) => {
                step_execution.status = StepStatus::ProcessorError;
                return Err(error);
            }
        };

        // Write the processed items
        match self.write_chunk(step_execution, &processed_items) {
            Ok(()) => Ok(()),
            Err(error) => {
                step_execution.status = StepStatus::WriteError;
                Err(error)
            }
        }
    }

    /// Reads a chunk of items from the reader.
    ///
    /// This method attempts to read up to `chunk_size` items from the reader.
    /// It stops when either:
    /// - The chunk is full (reached `chunk_size` items)
    /// - There are no more items to read
    /// - The error skip limit is reached
    ///
    /// # Parameters
    /// - `read_items`: Vector to store the read items
    ///
    /// # Returns
    /// - `Ok(ChunkStatus::Full)`: The chunk is full with `chunk_size` items
    /// - `Ok(ChunkStatus::Finished)`: There are no more items to read
    /// - `Err(BatchError)`: An error occurred and skip limit was reached
    fn read_chunk(
        &self,
        step_execution: &mut StepExecution,
    ) -> Result<(Vec<I>, ChunkStatus), BatchError> {
        let start = Instant::now();
        let result = self.read_chunk_inner(step_execution);
        step_execution.read_duration += start.elapsed();
        result
    }

    // timed by the wrapper above
    fn read_chunk_inner(
        &self,
        step_execution: &mut StepExecution,
    ) -> Result<(Vec<I>, ChunkStatus), BatchError> {
        debug!("Start reading chunk");

        let mut read_items = Vec::with_capacity(self.chunk_size as usize);

        loop {
            let read_result = self.reader.read();

            match read_result {
                Ok(item) => {
                    match item {
                        Some(item) => {
                            read_items.push(item);
                            step_execution.read_count += 1;

                            if read_items.len() >= self.chunk_size as usize {
                                return Ok((read_items, ChunkStatus::Full));
                            }
                        }
                        None => {
                            if read_items.is_empty() {
                                return Ok((read_items, ChunkStatus::Finished));
                            } else {
                                return Ok((read_items, ChunkStatus::Full));
                            }
                        }
                    };
                }
                Err(error) => {
                    warn!("Error reading item: {}", error);
                    step_execution.read_error_count += 1;

                    if self.is_skip_limit_reached(step_execution) {
                        // Set the status to ReadError when we hit the limit
                        step_execution.status = StepStatus::ReadError;
                        return Err(error);
                    }
                }
            }
        }
    }

    /// Processes a chunk of items using the processor.
    ///
    /// This method applies the processor to each item in the input chunk.
    /// It collects the successfully processed items and tracks any errors.
    ///
    /// # Parameters
    /// - `read_items`: Vector of items to process
    ///
    /// # Returns
    /// - `Ok(Vec<W>)`: Vector of successfully processed items
    /// - `Err(BatchError)`: An error occurred and skip limit was reached
    fn process_chunk(
        &self,
        step_execution: &mut StepExecution,
        read_items: Vec<I>,
    ) -> Result<Vec<O>, BatchError> {
        let start = Instant::now();
        let result = self.process_chunk_inner(step_execution, read_items);
        step_execution.process_duration += start.elapsed();
        result
    }

    // timed by the wrapper above
    fn process_chunk_inner(
        &self,
        step_execution: &mut StepExecution,
        read_items: Vec<I>,
    ) -> Result<Vec<O>, BatchError> {
        debug!("Processing chunk of {} items", read_items.len());
        let mut result = Vec::with_capacity(read_items.len());

        for item in read_items {
            match self.processor.process(item) {
                Ok(Some(processed_item)) => {
                    result.push(processed_item);
                    step_execution.process_count += 1;
                }
                Ok(None) => {
                    step_execution.filter_count += 1;
                    debug!("Item filtered by processor");
                }
                Err(error) => {
                    warn!("Error processing item: {}", error);
                    step_execution.process_error_count += 1;

                    if self.is_skip_limit_reached(step_execution) {
                        // Set the status to ProcessorError when we hit the limit
                        step_execution.status = StepStatus::ProcessorError;
                        return Err(error);
                    }
                }
            }
        }

        Ok(result)
    }

    /// Writes a chunk of processed items using the writer.
    ///
    /// This method writes the processed items to the destination
    /// and handles any errors that occur.
    ///
    /// # Parameters
    /// - `processed_items`: Vector of items to write
    ///
    /// # Returns
    /// - `Ok(())`: All items were written successfully
    /// - `Err(BatchError)`: An error occurred and skip limit was reached
    fn write_chunk(
        &self,
        step_execution: &mut StepExecution,
        processed_items: &[O],
    ) -> Result<(), BatchError> {
        debug!("Writing chunk of {} items", processed_items.len());

        if processed_items.is_empty() {
            debug!("No items to write, skipping write call");
            return Ok(());
        }

        let write_start = Instant::now();
        let write_result = self.writer.write(processed_items);
        step_execution.write_duration += write_start.elapsed();

        match write_result {
            Ok(()) => {
                step_execution.write_count += processed_items.len();

                let flush_start = Instant::now();
                let flush_result = self.writer.flush();
                step_execution.flush_duration += flush_start.elapsed();
                Self::manage_error(flush_result);

                Ok(())
            }
            Err(error) => {
                warn!("Error writing items: {}", error);
                step_execution.write_error_count += processed_items.len();

                if self.is_skip_limit_reached(step_execution) {
                    // Set the status to WriteError to indicate a write failure
                    step_execution.status = StepStatus::WriteError;
                    return Err(error);
                }
                Ok(())
            }
        }
    }

    fn is_skip_limit_reached(&self, step_execution: &StepExecution) -> bool {
        step_execution.read_error_count
            + step_execution.write_error_count
            + step_execution.process_error_count
            > self.skip_limit.into()
    }
    /// Helper method to handle errors gracefully.
    ///
    /// This method is used to handle errors from operations where we want
    /// to log the error but not fail the step.
    ///
    /// # Parameters
    /// - `result`: Result to check for errors
    fn manage_error(result: Result<(), BatchError>) {
        if let Err(error) = result {
            warn!("Non-fatal error: {}", error);
        }
    }
}

/// Builder for creating ChunkOrientedStep instances.
///
/// Provides a fluent API for configuring chunk-oriented steps with validation
/// to ensure all required components (reader, processor, writer) are provided.
///
/// # Type Parameters
/// - `I`: The input item type (what the reader produces)
/// - `O`: The output item type (what the processor produces and writer consumes)
///
/// # Examples
///
/// ```rust
/// use spring_batch_rs::core::step::ChunkOrientedStepBuilder;
/// use spring_batch_rs::core::item::{ItemReader, ItemProcessor, ItemWriter};
/// use spring_batch_rs::BatchError;
///
/// # struct MyReader;
/// # impl ItemReader<i32> for MyReader {
/// #     fn read(&self) -> Result<Option<i32>, BatchError> { Ok(None) }
/// # }
/// # struct MyProcessor;
/// # impl ItemProcessor<i32, String> for MyProcessor {
/// #     fn process(&self, item: i32) -> Result<Option<String>, BatchError> { Ok(Some(item.to_string())) }
/// # }
/// # struct MyWriter;
/// # impl ItemWriter<String> for MyWriter {
/// #     fn write(&self, items: &[String]) -> Result<(), BatchError> { Ok(()) }
/// #     fn flush(&self) -> Result<(), BatchError> { Ok(()) }
/// #     fn open(&self) -> Result<(), BatchError> { Ok(()) }
/// #     fn close(&self) -> Result<(), BatchError> { Ok(()) }
/// # }
/// let reader = MyReader;
/// let processor = MyProcessor;
/// let writer = MyWriter;
///
/// let step = ChunkOrientedStepBuilder::new("number-to-string")
///     .reader(&reader)
///     .processor(&processor)
///     .writer(&writer)
///     .chunk_size(500)
///     .skip_limit(25)
///     .build();
/// ```
pub struct ChunkOrientedStepBuilder<'a, I, O> {
    /// Name for the step
    name: String,
    /// Component responsible for reading items from the source
    reader: Option<&'a dyn ItemReader<I>>,
    /// Component responsible for processing items
    processor: Option<&'a dyn ItemProcessor<I, O>>,
    /// Component responsible for writing items to the destination
    writer: Option<&'a dyn ItemWriter<O>>,
    /// Number of items to process in each chunk
    chunk_size: u16,
    /// Maximum number of errors allowed before failing the step
    skip_limit: u16,
}

impl<'a, I, O> ChunkOrientedStepBuilder<'a, I, O> {
    /// Creates a new ChunkOrientedStepBuilder with the specified name.
    ///
    /// Sets default values:
    /// - `chunk_size`: 10
    /// - `skip_limit`: 0 (no error tolerance)
    ///
    /// # Parameters
    /// - `name`: Human-readable name for the step
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::ChunkOrientedStepBuilder;
    ///
    /// let builder = ChunkOrientedStepBuilder::<String, String>::new("data-migration");
    /// ```
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            reader: None,
            processor: None,
            writer: None,
            chunk_size: 10,
            skip_limit: 0,
        }
    }

    /// Sets the item reader for this step.
    ///
    /// The reader is responsible for providing items to be processed.
    /// This is a required component for chunk-oriented steps.
    ///
    /// # Parameters
    /// - `reader`: Implementation of ItemReader that produces items of type `I`
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use spring_batch_rs::core::step::ChunkOrientedStepBuilder;
    /// # use spring_batch_rs::core::item::ItemReader;
    /// # use spring_batch_rs::BatchError;
    /// # struct FileReader;
    /// # impl ItemReader<String> for FileReader {
    /// #     fn read(&self) -> Result<Option<String>, BatchError> { Ok(None) }
    /// # }
    /// let reader = FileReader;
    /// let builder = ChunkOrientedStepBuilder::<String, String>::new("file-processing")
    ///     .reader(&reader);
    /// ```
    pub fn reader(mut self, reader: &'a dyn ItemReader<I>) -> Self {
        self.reader = Some(reader);
        self
    }

    /// Sets the item processor for this step.
    ///
    /// The processor transforms items from type `I` to type `O`.
    /// This is a required component for chunk-oriented steps.
    ///
    /// # Parameters
    /// - `processor`: Implementation of ItemProcessor that transforms items from `I` to `O`
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use spring_batch_rs::core::step::ChunkOrientedStepBuilder;
    /// # use spring_batch_rs::core::item::{ItemReader, ItemProcessor};
    /// # use spring_batch_rs::BatchError;
    /// # struct FileReader;
    /// # impl ItemReader<String> for FileReader {
    /// #     fn read(&self) -> Result<Option<String>, BatchError> { Ok(None) }
    /// # }
    /// # struct UppercaseProcessor;
    /// # impl ItemProcessor<String, String> for UppercaseProcessor {
    /// #     fn process(&self, item: String) -> Result<Option<String>, BatchError> { Ok(Some(item.to_uppercase())) }
    /// # }
    /// let reader = FileReader;
    /// let processor = UppercaseProcessor;
    /// let builder = ChunkOrientedStepBuilder::new("text-processing")
    ///     .reader(&reader)
    ///     .processor(&processor);
    /// ```
    pub fn processor(mut self, processor: &'a dyn ItemProcessor<I, O>) -> Self {
        self.processor = Some(processor);
        self
    }

    /// Sets the item writer for this step.
    ///
    /// The writer is responsible for persisting processed items.
    /// This is a required component for chunk-oriented steps.
    ///
    /// # Parameters
    /// - `writer`: Implementation of ItemWriter that consumes items of type `O`
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use spring_batch_rs::core::step::ChunkOrientedStepBuilder;
    /// # use spring_batch_rs::core::item::{ItemReader, ItemProcessor, ItemWriter};
    /// # use spring_batch_rs::BatchError;
    /// # struct FileReader;
    /// # impl ItemReader<String> for FileReader {
    /// #     fn read(&self) -> Result<Option<String>, BatchError> { Ok(None) }
    /// # }
    /// # struct UppercaseProcessor;
    /// # impl ItemProcessor<String, String> for UppercaseProcessor {
    /// #     fn process(&self, item: String) -> Result<Option<String>, BatchError> { Ok(Some(item.to_uppercase())) }
    /// # }
    /// # struct FileWriter;
    /// # impl ItemWriter<String> for FileWriter {
    /// #     fn write(&self, items: &[String]) -> Result<(), BatchError> { Ok(()) }
    /// #     fn flush(&self) -> Result<(), BatchError> { Ok(()) }
    /// #     fn open(&self) -> Result<(), BatchError> { Ok(()) }
    /// #     fn close(&self) -> Result<(), BatchError> { Ok(()) }
    /// # }
    /// let reader = FileReader;
    /// let processor = UppercaseProcessor;
    /// let writer = FileWriter;
    /// let builder = ChunkOrientedStepBuilder::new("file-processing")
    ///     .reader(&reader)
    ///     .processor(&processor)
    ///     .writer(&writer);
    /// ```
    pub fn writer(mut self, writer: &'a dyn ItemWriter<O>) -> Self {
        self.writer = Some(writer);
        self
    }

    /// Sets the chunk size for this step.
    ///
    /// The chunk size determines how many items are processed together
    /// in a single transaction. Larger chunks can improve performance
    /// but use more memory.
    ///
    /// # Parameters
    /// - `chunk_size`: Number of items to process per chunk (must be > 0)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::ChunkOrientedStepBuilder;
    ///
    /// let builder = ChunkOrientedStepBuilder::<String, String>::new("bulk-processing")
    ///     .chunk_size(1000); // Process 1000 items per chunk
    /// ```
    pub fn chunk_size(mut self, chunk_size: u16) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Sets the skip limit for this step.
    ///
    /// The skip limit determines how many errors are tolerated before
    /// the step fails. A value of 0 means no errors are tolerated.
    ///
    /// # Parameters
    /// - `skip_limit`: Maximum number of errors allowed
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::ChunkOrientedStepBuilder;
    ///
    /// let builder = ChunkOrientedStepBuilder::<String, String>::new("fault-tolerant-processing")
    ///     .skip_limit(100); // Allow up to 100 errors
    /// ```
    pub fn skip_limit(mut self, skip_limit: u16) -> Self {
        self.skip_limit = skip_limit;
        self
    }

    /// Builds the ChunkOrientedStep instance.
    ///
    /// # Panics
    /// Panics if any required component (reader, processor, writer) has not been set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use spring_batch_rs::core::step::ChunkOrientedStepBuilder;
    /// # use spring_batch_rs::core::item::{ItemReader, ItemProcessor, ItemWriter};
    /// # use spring_batch_rs::BatchError;
    /// # struct MyReader;
    /// # impl ItemReader<String> for MyReader {
    /// #     fn read(&self) -> Result<Option<String>, BatchError> { Ok(None) }
    /// # }
    /// # struct MyProcessor;
    /// # impl ItemProcessor<String, String> for MyProcessor {
    /// #     fn process(&self, item: String) -> Result<Option<String>, BatchError> { Ok(Some(item)) }
    /// # }
    /// # struct MyWriter;
    /// # impl ItemWriter<String> for MyWriter {
    /// #     fn write(&self, items: &[String]) -> Result<(), BatchError> { Ok(()) }
    /// #     fn flush(&self) -> Result<(), BatchError> { Ok(()) }
    /// #     fn open(&self) -> Result<(), BatchError> { Ok(()) }
    /// #     fn close(&self) -> Result<(), BatchError> { Ok(()) }
    /// # }
    /// let reader = MyReader;
    /// let processor = MyProcessor;
    /// let writer = MyWriter;
    ///
    /// let step = ChunkOrientedStepBuilder::new("complete-step")
    ///     .reader(&reader)
    ///     .processor(&processor)
    ///     .writer(&writer)
    ///     .chunk_size(500)
    ///     .skip_limit(10)
    ///     .build();
    /// ```
    pub fn build(self) -> ChunkOrientedStep<'a, I, O> {
        ChunkOrientedStep {
            name: self.name,
            reader: self.reader.expect("Reader is required for building a step"),
            processor: self
                .processor
                .expect("Processor is required for building a step"),
            writer: self.writer.expect("Writer is required for building a step"),
            chunk_size: self.chunk_size,
            skip_limit: self.skip_limit,
        }
    }
}

/// Main entry point for building steps of any type.
///
/// StepBuilder provides a unified interface for creating both chunk-oriented
/// and tasklet steps. It uses the builder pattern to provide a fluent API
/// for step configuration.
///
/// # Type Parameters
/// - `I`: The input item type for chunk-oriented steps
/// - `O`: The output item type for chunk-oriented steps
///
/// # Examples
///
/// ## Creating a Chunk-Oriented Step
///
/// ```rust
/// use spring_batch_rs::core::step::{StepBuilder, StepExecution, Step};
/// use spring_batch_rs::core::item::{ItemReader, ItemProcessor, ItemWriter};
/// use spring_batch_rs::BatchError;
///
/// # struct MyReader;
/// # impl ItemReader<String> for MyReader {
/// #     fn read(&self) -> Result<Option<String>, BatchError> { Ok(None) }
/// # }
/// # struct MyProcessor;
/// # impl ItemProcessor<String, String> for MyProcessor {
/// #     fn process(&self, item: String) -> Result<Option<String>, BatchError> { Ok(Some(item)) }
/// # }
/// # struct MyWriter;
/// # impl ItemWriter<String> for MyWriter {
/// #     fn write(&self, items: &[String]) -> Result<(), BatchError> { Ok(()) }
/// #     fn flush(&self) -> Result<(), BatchError> { Ok(()) }
/// #     fn open(&self) -> Result<(), BatchError> { Ok(()) }
/// #     fn close(&self) -> Result<(), BatchError> { Ok(()) }
/// # }
/// let reader = MyReader;
/// let processor = MyProcessor;
/// let writer = MyWriter;
///
/// let step = StepBuilder::new("data-processing")
///     .chunk::<String, String>(100)
///     .reader(&reader)
///     .processor(&processor)
///     .writer(&writer)
///     .build();
/// ```
///
/// ## Creating a Tasklet Step
///
/// ```rust
/// use spring_batch_rs::core::step::{StepBuilder, StepExecution, RepeatStatus, Tasklet};
/// use spring_batch_rs::BatchError;
///
/// # struct MyTasklet;
/// # impl Tasklet for MyTasklet {
/// #     fn execute(&self, _step_execution: &StepExecution) -> Result<RepeatStatus, BatchError> {
/// #         Ok(RepeatStatus::Finished)
/// #     }
/// # }
/// let tasklet = MyTasklet;
///
/// let step = StepBuilder::new("cleanup-task")
///     .tasklet(&tasklet)
///     .build();
/// ```
pub struct StepBuilder {
    name: String,
}

impl StepBuilder {
    /// Creates a new StepBuilder with the specified name.
    ///
    /// # Parameters
    /// - `name`: Human-readable name for the step
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::StepBuilder;
    ///
    /// let builder = StepBuilder::new("my-step");
    /// ```
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    /// Configures this step to use a tasklet for execution.
    ///
    /// Returns a TaskletBuilder for further configuration of the tasklet step.
    ///
    /// # Parameters
    /// - `tasklet`: The tasklet implementation to execute
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::{StepBuilder, StepExecution, RepeatStatus, Tasklet};
    /// use spring_batch_rs::BatchError;
    ///
    /// # struct FileCleanupTasklet;
    /// # impl Tasklet for FileCleanupTasklet {
    /// #     fn execute(&self, _step_execution: &StepExecution) -> Result<RepeatStatus, BatchError> {
    /// #         Ok(RepeatStatus::Finished)
    /// #     }
    /// # }
    /// let tasklet = FileCleanupTasklet;
    /// let step = StepBuilder::new("cleanup")
    ///     .tasklet(&tasklet)
    ///     .build();
    /// ```
    pub fn tasklet(self, tasklet: &dyn Tasklet) -> TaskletBuilder<'_> {
        TaskletBuilder::new(&self.name).tasklet(tasklet)
    }

    /// Configures this step for chunk-oriented processing.
    ///
    /// Returns a ChunkOrientedStepBuilder for further configuration of the chunk step.
    ///
    /// # Parameters
    /// - `chunk_size`: Number of items to process per chunk
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::{StepBuilder, Step};
    /// use spring_batch_rs::core::item::{ItemReader, ItemProcessor, ItemWriter};
    /// use spring_batch_rs::BatchError;
    ///
    /// # struct MyReader;
    /// # impl ItemReader<String> for MyReader {
    /// #     fn read(&self) -> Result<Option<String>, BatchError> { Ok(None) }
    /// # }
    /// # struct MyProcessor;
    /// # impl ItemProcessor<String, String> for MyProcessor {
    /// #     fn process(&self, item: String) -> Result<Option<String>, BatchError> { Ok(Some(item)) }
    /// # }
    /// # struct MyWriter;
    /// # impl ItemWriter<String> for MyWriter {
    /// #     fn write(&self, items: &[String]) -> Result<(), BatchError> { Ok(()) }
    /// #     fn flush(&self) -> Result<(), BatchError> { Ok(()) }
    /// #     fn open(&self) -> Result<(), BatchError> { Ok(()) }
    /// #     fn close(&self) -> Result<(), BatchError> { Ok(()) }
    /// # }
    /// let reader = MyReader;
    /// let processor = MyProcessor;
    /// let writer = MyWriter;
    ///
    /// let step = StepBuilder::new("bulk-processing")
    ///     .chunk(1000)  // Process 1000 items per chunk
    ///     .reader(&reader)
    ///     .processor(&processor)
    ///     .writer(&writer)
    ///     .build();
    /// ```
    pub fn chunk<'a, I, O>(self, chunk_size: u16) -> ChunkOrientedStepBuilder<'a, I, O> {
        ChunkOrientedStepBuilder::new(&self.name).chunk_size(chunk_size)
    }
}

/// Represents the status of a chunk during processing.
///
/// This enum indicates whether a chunk has been fully processed or if
/// there are more items to process. It's used internally by the step
/// execution logic to control the processing loop.
///
/// # Examples
///
/// ```rust
/// use spring_batch_rs::core::step::ChunkStatus;
///
/// let status = ChunkStatus::Full;
/// match status {
///     ChunkStatus::Full => println!("Chunk is ready for processing"),
///     ChunkStatus::Finished => println!("No more items to process"),
/// }
/// ```
#[derive(Debug, PartialEq)]
pub enum ChunkStatus {
    /// The chunk has been fully processed.
    ///
    /// This indicates that there are no more items to process in the current
    /// data source (typically because we've reached the end of the input).
    /// The step should complete after processing any remaining items.
    Finished,

    /// The chunk is full and ready to be processed.
    ///
    /// This indicates that we've collected a full chunk of items (based on
    /// the configured chunk size) and they are ready to be processed.
    /// The step should continue reading more chunks after processing this one.
    Full,
}

/// Represents the current status of a step execution.
///
/// This enum indicates the current state of a step execution, including
/// both success and various failure states. It helps track the step's
/// progress and identify the cause of any failures.
///
/// # Examples
///
/// ```rust
/// use spring_batch_rs::core::step::{StepExecution, StepStatus};
///
/// let mut step_execution = StepExecution::new("my-step");
/// assert_eq!(step_execution.status, StepStatus::Starting);
///
/// // After successful execution
/// step_execution.status = StepStatus::Success;
/// match step_execution.status {
///     StepStatus::Success => println!("Step completed successfully"),
///     StepStatus::ReadError => println!("Failed during reading"),
///     StepStatus::ProcessorError => println!("Failed during processing"),
///     StepStatus::WriteError => println!("Failed during writing"),
///     StepStatus::Starting => println!("Step is starting"),
///     StepStatus::Failed => println!("Step has failed"),
///     StepStatus::Started => println!("Step has started"),
/// }
/// ```
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum StepStatus {
    /// The step executed successfully.
    ///
    /// All items were read, processed, and written without errors
    /// exceeding configured skip limits. This is the desired end state
    /// for a step execution.
    Success,

    /// An error occurred during the read operation.
    ///
    /// This indicates that an error occurred while reading items from the
    /// source, and the error count exceeded the configured skip limit.
    /// The step was terminated due to too many read failures.
    ReadError,

    /// An error occurred during the processing operation.
    ///
    /// This indicates that an error occurred while processing items, and
    /// the error count exceeded the configured skip limit.
    /// The step was terminated due to too many processing failures.
    ProcessorError,

    /// An error occurred during the write operation.
    ///
    /// This indicates that an error occurred while writing items to the
    /// destination, and the error count exceeded the configured skip limit.
    /// The step was terminated due to too many write failures.
    WriteError,

    /// The step is starting.
    ///
    /// This is the initial state of a step before execution begins.
    /// All steps start in this state when first created.
    Starting,

    /// The step is failed.
    ///
    /// This is the final state of a step after execution has failed.
    Failed,

    /// The step is started.
    ///
    /// This is the state of a step after execution has started.
    Started,
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use mockall::mock;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    use crate::{
        BatchError,
        core::{
            item::{
                ItemProcessor, ItemProcessorResult, ItemReader, ItemReaderResult, ItemWriter,
                ItemWriterResult, PassThroughProcessor,
            },
            step::{StepExecution, StepStatus},
        },
    };

    use super::{
        BatchStatus, ChunkOrientedStepBuilder, ChunkStatus, RepeatStatus, Step, StepBuilder,
        Tasklet, TaskletBuilder,
    };

    mock! {
        pub TestItemReader {}
        impl ItemReader<Car> for TestItemReader {
            fn read(&self) -> ItemReaderResult<Car>;
        }
    }

    mock! {
        pub TestProcessor {}
        impl ItemProcessor<Car, Car> for TestProcessor {
            fn process(&self, item: Car) -> ItemProcessorResult<Car>;
        }
    }

    mock! {
        pub TestItemWriter {}
        impl ItemWriter<Car> for TestItemWriter {
            fn write(&self, items: &[Car]) -> ItemWriterResult;
            fn flush(&self) -> ItemWriterResult;
            fn open(&self) -> ItemWriterResult;
            fn close(&self) -> ItemWriterResult;
        }
    }

    mock! {
        pub TestTasklet {}
        impl Tasklet for TestTasklet {
            fn execute(&self, step_execution: &StepExecution) -> Result<RepeatStatus, BatchError>;
        }
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    struct Car {
        year: u16,
        make: String,
        model: String,
        description: String,
    }

    fn mock_read(i: &mut u16, error_count: u16, end_count: u16) -> ItemReaderResult<Car> {
        if end_count > 0 && *i == end_count {
            return Ok(None);
        } else if error_count > 0 && *i == error_count {
            return Err(BatchError::ItemReader("mock read error".to_string()));
        }

        let car = Car {
            year: 1979,
            make: "make".to_owned(),
            model: "model".to_owned(),
            description: "description".to_owned(),
        };
        *i += 1;
        Ok(Some(car))
    }

    fn sample_car() -> Option<Car> {
        Some(Car {
            year: 2024,
            make: "Renault".to_string(),
            model: "Zoe".to_string(),
            description: "electric".to_string(),
        })
    }

    fn mock_process(i: &mut u16, error_at: &[u16]) -> ItemProcessorResult<Car> {
        *i += 1;
        if error_at.contains(i) {
            return Err(BatchError::ItemProcessor("mock process error".to_string()));
        }

        let car = Car {
            year: 1979,
            make: "make".to_owned(),
            model: "model".to_owned(),
            description: "description".to_owned(),
        };
        Ok(Some(car))
    }

    #[test]
    fn step_should_succeded_with_empty_data() -> Result<()> {
        let mut reader = MockTestItemReader::default();
        let reader_result = Ok(None);
        reader.expect_read().return_once(move || reader_result);

        let mut processor = MockTestProcessor::default();
        processor.expect_process().never();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never();
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step.get_name(), "test");
        assert!(!step.get_name().is_empty());
        assert!(!step_execution.id.is_nil());
        assert_eq!(step_execution.status, StepStatus::Success);

        Ok(())
    }

    #[test]
    fn step_should_failed_with_processor_error() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 4));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[2]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never();
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_err());
        assert_eq!(step_execution.status, StepStatus::ProcessorError);

        Ok(())
    }

    #[test]
    fn step_should_failed_with_write_error() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 4));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        let result = Err(BatchError::ItemWriter("mock write error".to_string()));
        writer.expect_write().return_once(move |_| result);
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_err());
        assert_eq!(step_execution.status, StepStatus::WriteError);

        Ok(())
    }

    #[test]
    fn step_should_succeed_even_with_processor_error() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 4));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[2]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(2).returning(|_| Ok(()));
        writer.expect_flush().times(2).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(1)
            .build();

        let mut step_execution = StepExecution::new(step.get_name());

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);

        Ok(())
    }

    #[test]
    fn step_should_fail_with_read_error() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 1, 4));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never();
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_err());
        assert_eq!(step_execution.status, StepStatus::ReadError);
        assert_eq!(step_execution.read_error_count, 1);

        Ok(())
    }

    #[test]
    fn step_should_respect_chunk_size() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 6));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(2).returning(|_| Ok(()));
        writer.expect_flush().times(2).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.read_count, 6);
        assert_eq!(step_execution.write_count, 6);

        Ok(())
    }

    #[test]
    fn step_should_track_error_counts() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 4));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[1, 2]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(2).returning(|_| Ok(()));
        writer.expect_flush().times(2).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(2)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.process_error_count, 2);

        Ok(())
    }

    #[test]
    fn step_should_measure_execution_time() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 2));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(1).returning(|_| Ok(()));
        writer.expect_flush().times(1).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert!(step_execution.duration.unwrap().as_nanos() > 0);
        assert!(step_execution.start_time.unwrap() <= step_execution.end_time.unwrap());

        Ok(())
    }

    #[test]
    fn step_should_handle_empty_chunk_at_end() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 1));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(1).returning(|items| {
            assert_eq!(items.len(), 1); // Partial chunk with 1 item
            Ok(())
        });
        writer.expect_flush().times(1).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.read_count, 1);
        assert_eq!(step_execution.write_count, 1);

        Ok(())
    }

    #[test]
    fn step_execution_should_initialize_correctly() -> Result<()> {
        let step_execution = StepExecution::new("test_step");

        assert_eq!(step_execution.name, "test_step");
        assert_eq!(step_execution.status, StepStatus::Starting);
        assert!(step_execution.start_time.is_none());
        assert!(step_execution.end_time.is_none());
        assert!(step_execution.duration.is_none());
        assert_eq!(step_execution.read_count, 0);
        assert_eq!(step_execution.write_count, 0);
        assert_eq!(step_execution.read_error_count, 0);
        assert_eq!(step_execution.process_count, 0);
        assert_eq!(step_execution.process_error_count, 0);
        assert_eq!(step_execution.write_error_count, 0);
        assert!(!step_execution.id.is_nil());

        Ok(())
    }

    #[test]
    fn should_format_phase_summary_with_percentages() {
        let mut step_execution = StepExecution::new("load-postgres");
        step_execution.duration = Some(Duration::from_secs(10));
        step_execution.read_duration = Duration::from_secs(5);
        step_execution.process_duration = Duration::from_secs(1);
        step_execution.write_duration = Duration::from_secs(3);
        step_execution.flush_duration = Duration::from_secs(1);

        let summary = step_execution.phase_summary();

        assert!(summary.contains("load-postgres"), "summary: {summary}");
        assert!(summary.contains("read 5.0s (50%)"), "summary: {summary}");
        assert!(summary.contains("process 1.0s (10%)"), "summary: {summary}");
        assert!(summary.contains("write 3.0s (30%)"), "summary: {summary}");
        assert!(summary.contains("flush 1.0s (10%)"), "summary: {summary}");
    }

    #[test]
    fn should_report_zero_percentages_when_duration_is_unset() {
        let step_execution = StepExecution::new("never-ran");

        let summary = step_execution.phase_summary();

        assert!(summary.contains("(0%)"), "summary: {summary}");
        assert!(
            !summary.contains("NaN"),
            "division by zero leaked: {summary}"
        );
        assert!(
            !summary.contains("inf"),
            "division by zero leaked: {summary}"
        );
    }

    #[test]
    fn tasklet_step_should_execute_successfully() -> Result<()> {
        let mut tasklet = MockTestTasklet::default();
        tasklet
            .expect_execute()
            .times(1)
            .returning(|_| Ok(RepeatStatus::Finished));

        let step = StepBuilder::new("tasklet_test").tasklet(&tasklet).build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step.get_name(), "tasklet_test");

        Ok(())
    }

    #[test]
    fn tasklet_step_should_handle_tasklet_error() -> Result<()> {
        let mut tasklet = MockTestTasklet::default();
        tasklet
            .expect_execute()
            .times(1)
            .returning(|_| Err(BatchError::Step("tasklet error".to_string())));

        let step = StepBuilder::new("tasklet_test").tasklet(&tasklet).build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        // The tasklet step should now properly handle errors
        assert!(result.is_err());
        if let Err(BatchError::Step(msg)) = result {
            assert_eq!(msg, "tasklet error");
        } else {
            panic!("Expected Step error");
        }

        Ok(())
    }

    #[test]
    fn tasklet_step_should_handle_continuable_status() -> Result<()> {
        use std::cell::Cell;

        let call_count = Cell::new(0);
        let mut tasklet = MockTestTasklet::default();
        tasklet.expect_execute().times(4).returning(move |_| {
            let count = call_count.get();
            call_count.set(count + 1);
            if count < 3 {
                Ok(RepeatStatus::Continuable)
            } else {
                Ok(RepeatStatus::Finished)
            }
        });

        let step = StepBuilder::new("continuable_tasklet_test")
            .tasklet(&tasklet)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step.get_name(), "continuable_tasklet_test");

        Ok(())
    }

    #[test]
    fn tasklet_step_should_handle_multiple_continuable_cycles() -> Result<()> {
        use std::cell::Cell;

        let call_count = Cell::new(0);
        let mut tasklet = MockTestTasklet::default();

        // Set up a sequence: 5 Continuable calls -> 1 Finished call
        tasklet.expect_execute().times(6).returning(move |_| {
            let count = call_count.get();
            call_count.set(count + 1);
            if count < 5 {
                Ok(RepeatStatus::Continuable)
            } else {
                Ok(RepeatStatus::Finished)
            }
        });

        let step = StepBuilder::new("multi_cycle_tasklet_test")
            .tasklet(&tasklet)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step.get_name(), "multi_cycle_tasklet_test");

        Ok(())
    }

    #[test]
    fn tasklet_step_should_handle_error_after_continuable() -> Result<()> {
        use std::cell::Cell;

        let call_count = Cell::new(0);
        let mut tasklet = MockTestTasklet::default();

        // Set up a sequence: 2 Continuable calls -> 1 Error
        tasklet.expect_execute().times(3).returning(move |_| {
            let count = call_count.get();
            call_count.set(count + 1);
            if count < 2 {
                Ok(RepeatStatus::Continuable)
            } else {
                Err(BatchError::Step("error after continuable".to_string()))
            }
        });

        let step = StepBuilder::new("error_after_continuable_test")
            .tasklet(&tasklet)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_err());
        if let Err(BatchError::Step(msg)) = result {
            assert_eq!(msg, "error after continuable");
        } else {
            panic!("Expected Step error");
        }

        Ok(())
    }

    #[test]
    fn tasklet_step_should_handle_immediate_finished_status() -> Result<()> {
        let mut tasklet = MockTestTasklet::default();
        tasklet
            .expect_execute()
            .times(1)
            .returning(|_| Ok(RepeatStatus::Finished));

        let step = StepBuilder::new("immediate_finished_test")
            .tasklet(&tasklet)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step.get_name(), "immediate_finished_test");

        Ok(())
    }

    #[test]
    fn tasklet_step_should_access_step_execution_context() -> Result<()> {
        let mut tasklet = MockTestTasklet::default();
        tasklet
            .expect_execute()
            .times(1)
            .withf(|step_execution| {
                // Verify that the tasklet receives the correct step execution context
                step_execution.name == "context_test"
                    && step_execution.status == StepStatus::Started
            })
            .returning(|_| Ok(RepeatStatus::Finished));

        let step = StepBuilder::new("context_test").tasklet(&tasklet).build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn tasklet_builder_should_create_valid_tasklet_step() -> Result<()> {
        let mut tasklet = MockTestTasklet::default();
        tasklet
            .expect_execute()
            .times(1)
            .returning(|_| Ok(RepeatStatus::Finished));

        let step = TaskletBuilder::new("builder_test")
            .tasklet(&tasklet)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step.get_name(), "builder_test");

        Ok(())
    }

    #[test]
    fn tasklet_builder_should_panic_without_tasklet() {
        let result = std::panic::catch_unwind(|| TaskletBuilder::new("test").build());

        assert!(result.is_err());
    }

    #[test]
    fn step_should_handle_writer_open_error() -> Result<()> {
        let mut reader = MockTestItemReader::default();
        let reader_result = Ok(None);
        reader.expect_read().return_once(move || reader_result);

        let mut processor = MockTestProcessor::default();
        processor.expect_process().never();

        let mut writer = MockTestItemWriter::default();
        writer
            .expect_open()
            .times(1)
            .returning(|| Err(BatchError::ItemWriter("open error".to_string())));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        // The step should still succeed as open errors are managed
        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);

        Ok(())
    }

    #[test]
    fn step_should_handle_writer_close_error() -> Result<()> {
        let mut reader = MockTestItemReader::default();
        let reader_result = Ok(None);
        reader.expect_read().return_once(move || reader_result);

        let mut processor = MockTestProcessor::default();
        processor.expect_process().never();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never();
        writer
            .expect_close()
            .times(1)
            .returning(|| Err(BatchError::ItemWriter("close error".to_string())));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        // The step should still succeed as close errors are managed
        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);

        Ok(())
    }

    #[test]
    fn step_should_handle_writer_flush_error() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 2));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(1).returning(|_| Ok(()));
        writer
            .expect_flush()
            .times(1)
            .returning(|| Err(BatchError::ItemWriter("flush error".to_string())));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        // The step should still succeed as flush errors are managed
        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);

        Ok(())
    }

    #[test]
    fn step_should_handle_multiple_chunks_with_exact_chunk_size() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 6));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(2).returning(|items| {
            assert_eq!(items.len(), 3); // Each chunk should have exactly 3 items
            Ok(())
        });
        writer.expect_flush().times(2).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.read_count, 6);
        assert_eq!(step_execution.write_count, 6);

        Ok(())
    }

    #[test]
    fn step_should_handle_skip_limit_boundary() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 4));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[1, 2])); // 2 errors

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(2).returning(|_| Ok(()));
        writer.expect_flush().times(2).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(2) // Exactly at the limit
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.process_error_count, 2);

        Ok(())
    }

    #[test]
    fn step_should_fail_when_skip_limit_exceeded() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 4));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[1, 2, 3])); // 3 errors

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never(); // Should not reach write due to error
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(2) // Exceeded by 1
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_err());
        assert_eq!(step_execution.status, StepStatus::ProcessorError);
        assert_eq!(step_execution.process_error_count, 3);

        Ok(())
    }

    #[test]
    fn step_should_handle_empty_processed_chunk() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 3));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[1, 2, 3, 4])); // All items fail processing

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never(); // Empty chunks are not written
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(3) // Allow all errors
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.process_error_count, 3);
        assert_eq!(step_execution.write_count, 0); // No items written

        Ok(())
    }

    #[test]
    fn chunk_status_should_be_comparable() {
        assert_eq!(ChunkStatus::Finished, ChunkStatus::Finished);
        assert_eq!(ChunkStatus::Full, ChunkStatus::Full);
        assert_ne!(ChunkStatus::Finished, ChunkStatus::Full);
    }

    #[test]
    fn step_status_should_be_comparable() {
        assert_eq!(StepStatus::Success, StepStatus::Success);
        assert_eq!(StepStatus::ReadError, StepStatus::ReadError);
        assert_eq!(StepStatus::ProcessorError, StepStatus::ProcessorError);
        assert_eq!(StepStatus::WriteError, StepStatus::WriteError);
        assert_eq!(StepStatus::Starting, StepStatus::Starting);

        assert_ne!(StepStatus::Success, StepStatus::ReadError);
        assert_ne!(StepStatus::ProcessorError, StepStatus::WriteError);
    }

    #[test]
    fn repeat_status_should_be_comparable() {
        assert_eq!(RepeatStatus::Continuable, RepeatStatus::Continuable);
        assert_eq!(RepeatStatus::Finished, RepeatStatus::Finished);
        assert_ne!(RepeatStatus::Continuable, RepeatStatus::Finished);
    }

    #[test]
    fn step_builder_should_create_chunk_oriented_step() -> Result<()> {
        let mut reader = MockTestItemReader::default();
        reader.expect_read().return_once(|| Ok(None));

        let mut processor = MockTestProcessor::default();
        processor.expect_process().never();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never();
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("builder_test")
            .chunk(5)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(10)
            .build();

        let mut step_execution = StepExecution::new(&step.name);
        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step.get_name(), "builder_test");

        Ok(())
    }

    #[test]
    fn step_should_handle_large_chunk_size() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 5));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(1).returning(|items| {
            assert_eq!(items.len(), 5); // All items in one chunk
            Ok(())
        });
        writer.expect_flush().times(1).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(100) // Chunk size larger than available items
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.read_count, 5);
        assert_eq!(step_execution.write_count, 5);

        Ok(())
    }

    #[test]
    fn step_should_handle_mixed_errors_within_skip_limit() -> Result<()> {
        use std::cell::Cell;

        let read_counter = Cell::new(0u16);
        let mut reader = MockTestItemReader::default();
        reader.expect_read().returning(move || {
            let current = read_counter.get();
            if current == 2 {
                read_counter.set(current + 1);
                Err(BatchError::ItemReader("read error".to_string()))
            } else {
                let mut i = current;
                let result = mock_read(&mut i, 0, 6);
                read_counter.set(i);
                result
            }
        });

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[2])); // 1 process error

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(2).returning(|_| Ok(()));
        writer.expect_flush().times(2).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(2) // Allow 1 read error + 1 process error
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.read_error_count, 1);
        assert_eq!(step_execution.process_error_count, 1);

        Ok(())
    }

    #[test]
    fn step_execution_should_be_cloneable() -> Result<()> {
        let step_execution = StepExecution::new("test_step");
        let cloned_execution = step_execution.clone();

        assert_eq!(step_execution.id, cloned_execution.id);
        assert_eq!(step_execution.name, cloned_execution.name);
        assert_eq!(step_execution.status, cloned_execution.status);
        assert_eq!(step_execution.read_count, cloned_execution.read_count);
        assert_eq!(step_execution.write_count, cloned_execution.write_count);

        Ok(())
    }

    #[test]
    fn step_should_handle_zero_chunk_size() -> Result<()> {
        let mut reader = MockTestItemReader::default();
        reader.expect_read().return_once(|| Ok(None));

        let mut processor = MockTestProcessor::default();
        processor.expect_process().never();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never();
        writer.expect_close().times(1).returning(|| Ok(()));

        // Test with chunk size of 1 (minimum practical value)
        let step = StepBuilder::new("test")
            .chunk(1)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);

        Ok(())
    }

    #[test]
    fn step_should_handle_continuous_read_errors_until_skip_limit() -> Result<()> {
        use std::cell::Cell;

        let counter = Cell::new(0u16);
        let mut reader = MockTestItemReader::default();
        reader.expect_read().returning(move || {
            let current = counter.get();
            counter.set(current + 1);
            if current < 3 {
                Err(BatchError::ItemReader("continuous read error".to_string()))
            } else {
                Ok(None) // End of data after errors
            }
        });

        let mut processor = MockTestProcessor::default();
        processor.expect_process().never();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never();
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(2) // Should fail after 3 errors (exceeds limit of 2)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_err());
        assert_eq!(step_execution.status, StepStatus::ReadError);
        assert_eq!(step_execution.read_error_count, 3);

        Ok(())
    }

    #[test]
    fn step_should_handle_write_error_with_skip_limit() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 4));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer
            .expect_write()
            .times(1)
            .returning(|_| Err(BatchError::ItemWriter("write error".to_string())));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(0) // No tolerance for errors
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_err());
        assert_eq!(step_execution.status, StepStatus::WriteError);
        assert_eq!(step_execution.write_error_count, 3); // All items in chunk failed

        Ok(())
    }

    #[test]
    fn step_should_succeed_when_write_error_within_skip_limit() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 3));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer
            .expect_write()
            .times(1)
            .returning(|_| Err(BatchError::ItemWriter("write error".to_string())));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(3) // Exactly at limit: 3 write errors <= skip_limit(3), step continues
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.write_error_count, 3);
        assert_eq!(step_execution.write_count, 0); // No successful writes

        Ok(())
    }

    #[test]
    fn step_should_handle_partial_chunk_at_end() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 2)); // Only 2 items, chunk size is 3

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(1).returning(|items| {
            assert_eq!(items.len(), 2); // Partial chunk with 2 items
            Ok(())
        });
        writer.expect_flush().times(1).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.read_count, 2);
        assert_eq!(step_execution.write_count, 2);

        Ok(())
    }

    #[test]
    fn batch_status_should_have_all_variants() {
        // Test that all BatchStatus variants exist and can be created
        let _completed = BatchStatus::COMPLETED;
        let _starting = BatchStatus::STARTING;
        let _started = BatchStatus::STARTED;
        let _stopping = BatchStatus::STOPPING;
        let _stopped = BatchStatus::STOPPED;
        let _failed = BatchStatus::FAILED;
        let _abandoned = BatchStatus::ABANDONED;
        let _unknown = BatchStatus::UNKNOWN;
    }

    #[test]
    fn tasklet_builder_should_require_tasklet() {
        let mut tasklet = MockTestTasklet::default();
        tasklet.expect_execute().never();

        // This test documents that the builder panics if tasklet is not set
        // In a real scenario, this would be caught at compile time or runtime
        let builder = TaskletBuilder::new("test").tasklet(&tasklet);
        let _step = builder.build(); // Should not panic with tasklet set
    }

    #[test]
    fn chunk_oriented_step_builder_should_require_all_components() -> Result<()> {
        let mut reader = MockTestItemReader::default();
        reader.expect_read().return_once(|| Ok(None));

        let mut processor = MockTestProcessor::default();
        processor.expect_process().never();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never();
        writer.expect_close().times(1).returning(|| Ok(()));

        // Test that builder works with all required components
        let step = ChunkOrientedStepBuilder::new("test")
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .chunk_size(10)
            .skip_limit(5)
            .build();

        let mut step_execution = StepExecution::new(&step.name);
        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step.get_name(), "test");

        Ok(())
    }

    #[test]
    fn step_should_handle_maximum_skip_limit() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 3)); // Only 3 items to match chunk size

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[1, 2, 3])); // All items fail

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never(); // No items to write since all fail processing
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(u16::MAX) // Maximum skip limit
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.process_error_count, 3);

        Ok(())
    }

    #[test]
    fn step_should_handle_tasklet_step_timing() -> Result<()> {
        let mut tasklet = MockTestTasklet::default();
        tasklet
            .expect_execute()
            .times(1)
            .returning(|_| Ok(RepeatStatus::Finished));

        let step = StepBuilder::new("timing_test").tasklet(&tasklet).build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert!(step_execution.start_time.is_some());
        assert!(step_execution.end_time.is_some());
        assert!(step_execution.duration.is_some());
        assert!(step_execution.duration.unwrap().as_nanos() > 0);

        Ok(())
    }

    #[test]
    fn step_should_handle_tasklet_step_status_transitions() -> Result<()> {
        let mut tasklet = MockTestTasklet::default();
        tasklet
            .expect_execute()
            .times(1)
            .returning(|step_execution| {
                // Verify the step status is Started when tasklet is called
                assert_eq!(step_execution.status, StepStatus::Started);
                Ok(RepeatStatus::Finished)
            });

        let step = StepBuilder::new("status_test").tasklet(&tasklet).build();

        let mut step_execution = StepExecution::new(&step.name);
        assert_eq!(step_execution.status, StepStatus::Starting);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);

        Ok(())
    }

    #[test]
    fn step_should_handle_tasklet_step_failed_status() -> Result<()> {
        let mut tasklet = MockTestTasklet::default();
        tasklet
            .expect_execute()
            .times(1)
            .returning(|_| Err(BatchError::Step("tasklet failure".to_string())));

        let step = StepBuilder::new("failed_test").tasklet(&tasklet).build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_err());
        assert_eq!(step_execution.status, StepStatus::Failed);
        assert!(step_execution.end_time.is_some());
        assert!(step_execution.duration.is_some());

        Ok(())
    }

    #[test]
    fn chunk_oriented_step_builder_should_panic_without_reader() {
        let mut processor = MockTestProcessor::default();
        processor.expect_process().never();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().never();

        let result = std::panic::catch_unwind(|| {
            ChunkOrientedStepBuilder::new("test")
                .processor(&processor)
                .writer(&writer)
                .build()
        });

        assert!(result.is_err());
    }

    #[test]
    fn chunk_oriented_step_builder_should_panic_without_processor() {
        let mut reader = MockTestItemReader::default();
        reader.expect_read().never();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().never();

        let result = std::panic::catch_unwind(|| {
            ChunkOrientedStepBuilder::new("test")
                .reader(&reader)
                .writer(&writer)
                .build()
        });

        assert!(result.is_err());
    }

    #[test]
    fn chunk_oriented_step_builder_should_panic_without_writer() {
        let mut reader = MockTestItemReader::default();
        reader.expect_read().never();

        let mut processor = MockTestProcessor::default();
        processor.expect_process().never();

        let result = std::panic::catch_unwind(|| {
            ChunkOrientedStepBuilder::new("test")
                .reader(&reader)
                .processor(&processor)
                .build()
        });

        assert!(result.is_err());
    }

    #[test]
    fn step_should_handle_read_chunk_with_full_chunk() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 4)); // 4 items total

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[]));

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(2).returning(|items| {
            // First chunk has 3 items, second chunk has 1 item
            assert!(items.len() <= 3);
            Ok(())
        });
        writer.expect_flush().times(2).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.read_count, 4);
        assert_eq!(step_execution.write_count, 4);

        Ok(())
    }

    #[test]
    fn step_should_handle_process_chunk_with_all_errors() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 3));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[1, 2, 3])); // All items fail

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never(); // No items to write since all fail processing
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(5) // Allow all errors
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.process_error_count, 3);
        assert_eq!(step_execution.write_count, 0);

        Ok(())
    }

    #[test]
    fn step_should_handle_write_chunk_with_empty_items() -> Result<()> {
        let mut reader = MockTestItemReader::default();
        reader.expect_read().return_once(|| Ok(None));

        let mut processor = MockTestProcessor::default();
        processor.expect_process().never();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never(); // No items to write
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.read_count, 0);
        assert_eq!(step_execution.write_count, 0);

        Ok(())
    }

    #[test]
    fn step_should_handle_is_skip_limit_reached_boundary_conditions() -> Result<()> {
        let mut i = 0;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 4));

        let mut processor = MockTestProcessor::default();
        let mut i = 0;
        processor
            .expect_process()
            .returning(move |_| mock_process(&mut i, &[1, 2])); // 2 errors

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().times(2).returning(|_| Ok(()));
        writer.expect_flush().times(2).returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .skip_limit(2) // Exactly at the limit
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.process_error_count, 2);

        Ok(())
    }

    #[test]
    fn step_should_handle_manage_error_with_various_errors() -> Result<()> {
        let mut reader = MockTestItemReader::default();
        reader.expect_read().return_once(|| Ok(None));

        let mut processor = MockTestProcessor::default();
        processor.expect_process().never();

        let mut writer = MockTestItemWriter::default();
        writer
            .expect_open()
            .times(1)
            .returning(|| Err(BatchError::ItemWriter("open error".to_string())));
        writer.expect_write().never();
        writer
            .expect_close()
            .times(1)
            .returning(|| Err(BatchError::ItemWriter("close error".to_string())));

        let step = StepBuilder::new("test")
            .chunk(3)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);

        let result = step.execute(&mut step_execution);

        // Should still succeed as open/close errors are managed
        assert!(result.is_ok());
        assert_eq!(step_execution.status, StepStatus::Success);

        Ok(())
    }

    #[test]
    fn step_execution_should_have_unique_ids() -> Result<()> {
        let step_execution1 = StepExecution::new("test1");
        let step_execution2 = StepExecution::new("test2");

        assert_ne!(step_execution1.id, step_execution2.id);

        Ok(())
    }

    #[test]
    fn step_execution_should_clone_with_same_values() -> Result<()> {
        let mut step_execution = StepExecution::new("test_step");
        step_execution.read_count = 10;
        step_execution.write_count = 8;
        step_execution.status = StepStatus::Success;

        let cloned_execution = step_execution.clone();

        assert_eq!(step_execution.id, cloned_execution.id);
        assert_eq!(step_execution.name, cloned_execution.name);
        assert_eq!(step_execution.status, cloned_execution.status);
        assert_eq!(step_execution.read_count, cloned_execution.read_count);
        assert_eq!(step_execution.write_count, cloned_execution.write_count);

        Ok(())
    }

    #[test]
    fn step_status_should_support_copy_trait() {
        let status1 = StepStatus::Success;
        let status2 = status1; // This should work because StepStatus implements Copy

        assert_eq!(status1, status2);
        assert_eq!(status1, StepStatus::Success); // Original should still be usable
    }

    #[test]
    fn step_status_should_support_debug_trait() {
        let status = StepStatus::ProcessorError;
        let debug_string = format!("{:?}", status);

        assert!(debug_string.contains("ProcessorError"));
    }

    #[test]
    fn chunk_status_should_support_debug_trait() {
        let status = ChunkStatus::Full;
        let debug_string = format!("{:?}", status);

        assert!(debug_string.contains("Full"));
    }

    #[test]
    fn repeat_status_should_support_debug_trait() {
        let status = RepeatStatus::Continuable;
        let debug_string = format!("{:?}", status);

        assert!(debug_string.contains("Continuable"));
    }

    #[test]
    fn step_should_count_filtered_items() -> Result<()> {
        // Reader returns 4 items (items 0,1,2,3), ends at 4
        let mut i = 0u16;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 4));

        // Processor filters item at position 2 (returns Ok(None))
        let mut j = 0u16;
        let mut processor = MockTestProcessor::default();
        processor.expect_process().returning(move |_| {
            j += 1;
            if j == 2 {
                return Ok(None); // filter this item
            }
            Ok(Some(Car {
                year: 1979,
                make: "make".to_owned(),
                model: "model".to_owned(),
                description: "description".to_owned(),
            }))
        });

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        // 3 items pass through (4 read - 1 filtered), written in one chunk
        writer.expect_write().times(1).returning(|items| {
            assert_eq!(items.len(), 3, "expected 3 items written after filtering");
            Ok(())
        });
        writer.expect_flush().returning(|| Ok(()));
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(10)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);
        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(step_execution.read_count, 4, "should have read 4 items");
        assert_eq!(
            step_execution.filter_count, 1,
            "should have filtered 1 item"
        );
        assert_eq!(
            step_execution.process_count, 3,
            "should have processed 3 items"
        );
        assert_eq!(step_execution.write_count, 3, "should have written 3 items");

        Ok(())
    }

    #[test]
    fn step_should_not_call_writer_when_all_items_filtered() -> Result<()> {
        let mut i = 0u16;
        let mut reader = MockTestItemReader::default();
        reader
            .expect_read()
            .returning(move || mock_read(&mut i, 0, 3));

        let mut processor = MockTestProcessor::default();
        processor.expect_process().returning(|_| Ok(None)); // filter every item

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().times(1).returning(|| Ok(()));
        writer.expect_write().never(); // must NOT be called
        writer.expect_close().times(1).returning(|| Ok(()));

        let step = StepBuilder::new("test")
            .chunk(10)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new(&step.name);
        let result = step.execute(&mut step_execution);

        assert!(result.is_ok());
        assert_eq!(
            step_execution.filter_count, 3,
            "all 3 items should be filtered"
        );
        assert_eq!(
            step_execution.process_count, 0,
            "no items should reach process_count"
        );
        assert_eq!(step_execution.write_count, 0, "nothing should be written");

        Ok(())
    }

    #[test]
    fn should_initialize_phase_durations_to_zero() {
        let step_execution = StepExecution::new("phase-init");

        assert_eq!(step_execution.read_duration, Duration::ZERO);
        assert_eq!(step_execution.process_duration, Duration::ZERO);
        assert_eq!(step_execution.write_duration, Duration::ZERO);
        assert_eq!(step_execution.flush_duration, Duration::ZERO);
    }

    #[test]
    fn should_record_nonzero_read_duration_after_step() {
        let mut reader = MockTestItemReader::default();
        let mut counter = 0u16;
        reader.expect_read().returning(move || {
            std::thread::sleep(Duration::from_millis(20));
            counter += 1;
            if counter > 2 {
                Ok(None)
            } else {
                Ok(sample_car())
            }
        });

        let processor = PassThroughProcessor::<Car>::new();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().returning(|| Ok(()));
        writer.expect_write().returning(|_| Ok(()));
        writer.expect_flush().returning(|| Ok(()));
        writer.expect_close().returning(|| Ok(()));

        let step = StepBuilder::new("read-timing")
            .chunk(10)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new("read-timing");
        step.execute(&mut step_execution).unwrap();

        assert!(
            step_execution.read_duration >= Duration::from_millis(40),
            "expected read_duration to cover 3 sleeping reads, got {:?}",
            step_execution.read_duration
        );
    }

    #[test]
    fn should_attribute_slow_reads_to_read_duration_not_process() {
        let mut reader = MockTestItemReader::default();
        let mut counter = 0u16;
        reader.expect_read().returning(move || {
            std::thread::sleep(Duration::from_millis(30));
            counter += 1;
            if counter > 2 {
                Ok(None)
            } else {
                Ok(sample_car())
            }
        });

        let processor = PassThroughProcessor::<Car>::new();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().returning(|| Ok(()));
        writer.expect_write().returning(|_| Ok(()));
        writer.expect_flush().returning(|| Ok(()));
        writer.expect_close().returning(|| Ok(()));

        let step = StepBuilder::new("read-attribution")
            .chunk(10)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new("read-attribution");
        step.execute(&mut step_execution).unwrap();

        assert!(
            step_execution.read_duration > step_execution.process_duration,
            "a sleeping reader must not have its time attributed to process: read={:?} process={:?}",
            step_execution.read_duration,
            step_execution.process_duration
        );
    }

    #[test]
    fn should_record_flush_duration_separately_from_write() {
        let mut reader = MockTestItemReader::default();
        let mut counter = 0u16;
        reader.expect_read().returning(move || {
            counter += 1;
            if counter > 2 {
                Ok(None)
            } else {
                Ok(sample_car())
            }
        });

        let processor = PassThroughProcessor::<Car>::new();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().returning(|| Ok(()));
        writer.expect_write().returning(|_| Ok(()));
        writer.expect_flush().returning(|| {
            std::thread::sleep(Duration::from_millis(30));
            Ok(())
        });
        writer.expect_close().returning(|| Ok(()));

        let step = StepBuilder::new("flush-timing")
            .chunk(10)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new("flush-timing");
        step.execute(&mut step_execution).unwrap();

        assert!(
            step_execution.flush_duration >= Duration::from_millis(30),
            "flush_duration should capture the sleeping flush, got {:?}",
            step_execution.flush_duration
        );
        assert!(
            step_execution.flush_duration > step_execution.write_duration,
            "a slow flush must not be attributed to write: flush={:?} write={:?}",
            step_execution.flush_duration,
            step_execution.write_duration
        );
    }

    #[test]
    fn should_attribute_duration_to_the_correct_phase() {
        let mut reader = MockTestItemReader::default();
        let mut counter = 0u16;
        reader.expect_read().returning(move || {
            counter += 1;
            if counter > 3 {
                Ok(None)
            } else {
                Ok(sample_car())
            }
        });

        let processor = PassThroughProcessor::<Car>::new();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().returning(|| Ok(()));
        writer.expect_write().returning(|_| {
            std::thread::sleep(Duration::from_millis(50));
            Ok(())
        });
        writer.expect_flush().returning(|| Ok(()));
        writer.expect_close().returning(|| Ok(()));

        let step = StepBuilder::new("phase-attribution")
            .chunk(10)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new("phase-attribution");
        step.execute(&mut step_execution).unwrap();

        assert!(
            step_execution.write_duration > step_execution.read_duration,
            "slow writer should dominate: write={:?} read={:?}",
            step_execution.write_duration,
            step_execution.read_duration
        );
        assert!(
            step_execution.write_duration > step_execution.process_duration,
            "slow writer should dominate: write={:?} process={:?}",
            step_execution.write_duration,
            step_execution.process_duration
        );
    }

    #[test]
    fn should_leave_write_duration_at_zero_for_empty_chunk() {
        let mut reader = MockTestItemReader::default();
        reader.expect_read().returning(|| Ok(None));

        let processor = PassThroughProcessor::<Car>::new();

        let mut writer = MockTestItemWriter::default();
        writer.expect_open().returning(|| Ok(()));
        writer.expect_close().returning(|| Ok(()));

        let step = StepBuilder::new("empty-chunk")
            .chunk(10)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let mut step_execution = StepExecution::new("empty-chunk");
        step.execute(&mut step_execution).unwrap();

        assert_eq!(
            step_execution.write_duration,
            Duration::ZERO,
            "the empty-chunk early return skips the writer entirely"
        );
        assert_eq!(step_execution.flush_duration, Duration::ZERO);
    }
}
