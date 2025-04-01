use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

/// Job status constants
pub const JOB_STATUS_PENDING: u8 = 0;
pub const JOB_STATUS_RUNNING: u8 = 1;
pub const JOB_STATUS_COMPLETED: u8 = 2;
pub const JOB_STATUS_FAILED: u8 = 3;

/// Represents the progress and status of a background job
#[derive(Debug, Serialize, Deserialize)]
pub struct JobProgress {
    /// Unique job identifier
    pub job_id: String,
    /// Total number of steps to process
    pub total_steps: usize,
    /// Number of steps processed so far
    #[serde(skip)]
    pub processed_steps: AtomicUsize,
    /// Number of completed steps so far
    #[serde(skip)]
    pub completed_steps: AtomicUsize,
    /// Serializable version of processed_steps for serde
    #[serde(rename = "processed_steps")]
    pub processed_steps_value: usize,
    /// Serializable version of completed_steps for serde
    #[serde(rename = "completed_steps")]
    pub completed_steps_value: usize,
    /// Job status: 0=pending, 1=running, 2=completed, 3=failed
    #[serde(skip)]
    pub status: AtomicU8,
    /// Serializable version of status for serde
    #[serde(rename = "status")]
    pub status_value: u8,
    /// Optional error message if job failed
    pub error_message: Option<String>,
    /// Timestamp when job started
    pub start_time: u64,
    /// Timestamp when job completed or failed (if applicable)
    pub end_time: Option<u64>,
}

// Manually implement Clone for JobProgress
impl Clone for JobProgress {
    fn clone(&self) -> Self {
        JobProgress {
            job_id: self.job_id.clone(),
            total_steps: self.total_steps,
            processed_steps: AtomicUsize::new(self.processed_steps.load(Ordering::Relaxed)),
            completed_steps: AtomicUsize::new(self.completed_steps.load(Ordering::Relaxed)),
            processed_steps_value: self.processed_steps_value,
            completed_steps_value: self.completed_steps_value,
            status: AtomicU8::new(self.status.load(Ordering::Relaxed)),
            status_value: self.status_value,
            error_message: self.error_message.clone(),
            start_time: self.start_time,
            end_time: self.end_time,
        }
    }
}

impl JobProgress {
    /// Create a new job progress tracker
    pub fn new(job_id: String, total_steps: usize) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        JobProgress {
            job_id,
            total_steps,
            processed_steps: AtomicUsize::new(0),
            completed_steps: AtomicUsize::new(0),
            processed_steps_value: 0,
            completed_steps_value: 0,
            status: AtomicU8::new(JOB_STATUS_PENDING),
            status_value: JOB_STATUS_PENDING,
            error_message: None,
            start_time: now,
            end_time: None,
        }
    }

    /// Get status as string
    pub fn status_str(&self) -> &'static str {
        match self.status.load(Ordering::Relaxed) {
            JOB_STATUS_PENDING => "pending",
            JOB_STATUS_RUNNING => "running",
            JOB_STATUS_COMPLETED => "completed",
            JOB_STATUS_FAILED => "failed",
            _ => "unknown",
        }
    }

    pub fn get_progress(&self) -> f64 {
        if self.total_steps > 0 {
            (self.processed_steps.load(Ordering::Relaxed) as f64 * 100.0) / self.total_steps as f64
        } else {
            0.0
        }
    }

    pub fn get_completion_ratio(&self) -> f64 {
        if self.total_steps > 0 {
            (self.completed_steps.load(Ordering::Relaxed) as f64 * 100.0) / self.total_steps as f64
        } else {
            0.0
        }
    }

    /// Increment processed steps by one
    pub fn increment_processed(&self) -> usize {
        self.processed_steps.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Increment completed steps by one
    pub fn increment_completed(&self) -> usize {
        self.completed_steps.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Get current processed steps count
    pub fn processed(&mut self) -> usize {
        self.processed_steps_value = self.processed_steps.load(Ordering::Relaxed);
        self.processed_steps_value
    }

    /// Get current completed steps count
    pub fn completed(&mut self) -> usize {
        self.completed_steps_value = self.completed_steps.load(Ordering::Relaxed);
        self.completed_steps_value
    }

    /// Mark job as running
    pub fn mark_running(&mut self) {
        self.status.store(JOB_STATUS_RUNNING, Ordering::Relaxed);
        self.status_value = JOB_STATUS_RUNNING;
        self.start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Mark job as completed
    pub fn mark_completed(&mut self) {
        self.status.store(JOB_STATUS_COMPLETED, Ordering::Relaxed);
        self.status_value = JOB_STATUS_COMPLETED;
        self.end_time = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
    }

    /// Mark job as failed with error message
    pub fn mark_failed(&mut self, error: &str) {
        self.status.store(JOB_STATUS_FAILED, Ordering::Relaxed);
        self.status_value = JOB_STATUS_FAILED;
        self.error_message = Some(error.to_string());
        self.end_time = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
    }

    /// serialize to bytes
    pub fn to_vec(&mut self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        // sync the value of atomic variables into the serializable fields
        self.processed_steps_value = self.processed_steps.load(Ordering::Relaxed);
        self.completed_steps_value = self.completed_steps.load(Ordering::Relaxed);
        self.status_value = self.status.load(Ordering::Relaxed);
        rmp_serde::to_vec(&self)
    }

    /// deserialize from bytes
    pub fn from_slice(&mut self, data: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        let v: JobProgress = rmp_serde::from_slice(data)?;
        // sync the value of serializable fields into the atomic variables
        self.processed_steps
            .store(v.processed_steps_value, Ordering::Relaxed);
        self.completed_steps
            .store(v.completed_steps_value, Ordering::Relaxed);
        self.status.store(v.status_value, Ordering::Relaxed);
        Ok(v)
    }
}
