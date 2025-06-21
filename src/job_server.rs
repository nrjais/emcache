use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Trait that all jobs must implement
#[async_trait::async_trait]
pub trait Job: Send + Sync {
    /// The name of the job for identification
    fn name(&self) -> &str;

    /// Execute the job
    async fn execute(&self) -> Result<()>;

    /// Handle graceful shutdown
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// Job configuration
#[derive(Debug, Clone)]
pub struct JobConfig {
    pub name: String,
    pub interval: Duration,
    pub retry_count: u32,
    pub retry_backoff: Duration,
}

/// Job handle for managing running jobs
#[derive(Debug)]
struct JobHandle {
    config: JobConfig,
    cancellation_token: CancellationToken,
    task_handle: tokio::task::JoinHandle<()>,
}

/// Job server that manages and schedules jobs
pub struct JobServer {
    jobs: Arc<RwLock<HashMap<String, JobHandle>>>,
    shutdown_token: CancellationToken,
}

impl JobServer {
    /// Create a new job server
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Register a job with the server
    pub async fn register_job<J>(&self, job: J, config: JobConfig) -> Result<()>
    where
        J: Job + 'static,
    {
        let job_name = config.name.clone();
        info!("Registering job: {}", job_name);

        let job = Arc::new(job);
        let cancellation_token = self.shutdown_token.child_token();
        let jobs_map = Arc::clone(&self.jobs);

        // Create the job execution task
        let task_handle = tokio::spawn(Self::run_job_loop(
            job,
            config.clone(),
            cancellation_token.clone(),
        ));

        let job_handle = JobHandle {
            config,
            cancellation_token,
            task_handle,
        };

        // Store the job handle
        let mut jobs = jobs_map.write().await;
        jobs.insert(job_name.clone(), job_handle);

        info!("Job registered successfully: {}", job_name);
        Ok(())
    }

    /// Run the job execution loop
    async fn run_job_loop<J>(job: Arc<J>, config: JobConfig, cancellation_token: CancellationToken)
    where
        J: Job + 'static,
    {
        let mut interval = interval(config.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!("Starting job loop for: {}", config.name);

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Job {} received shutdown signal", config.name);
                    break;
                }
                _ = interval.tick() => {
                    debug!("Executing job: {}", config.name);

                    if let Err(e) = Self::execute_job_with_retry(&job, &config).await {
                        error!("Job {} failed after retries: {}", config.name, e);
                    }
                }
            }
        }

        // Graceful shutdown
        info!("Shutting down job: {}", config.name);
        if let Err(e) = job.shutdown().await {
            error!("Error during job shutdown {}: {}", config.name, e);
        }

        info!("Job {} shutdown complete", config.name);
    }

    /// Execute a job with retry logic
    async fn execute_job_with_retry<J>(job: &Arc<J>, config: &JobConfig) -> Result<()>
    where
        J: Job,
    {
        let mut attempts = 0;
        let mut backoff = config.retry_backoff;

        loop {
            match job.execute().await {
                Ok(()) => {
                    if attempts > 0 {
                        info!("Job {} succeeded after {} retries", config.name, attempts);
                    }
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;
                    if attempts > config.retry_count {
                        error!(
                            "Job {} failed after {} attempts: {}",
                            config.name, attempts, e
                        );
                        return Err(e);
                    }

                    warn!(
                        "Job {} failed (attempt {}/{}): {}. Retrying in {:?}",
                        config.name, attempts, config.retry_count, e, backoff
                    );

                    sleep(backoff).await;
                    backoff = backoff.mul_f32(1.5).min(Duration::from_secs(300));
                    // Max 5 minutes
                }
            }
        }
    }

    /// Get the status of all jobs
    pub async fn get_job_status(&self) -> HashMap<String, String> {
        let jobs = self.jobs.read().await;
        let mut status = HashMap::new();

        for (name, handle) in jobs.iter() {
            let job_status = if handle.task_handle.is_finished() {
                "stopped".to_string()
            } else if handle.cancellation_token.is_cancelled() {
                "shutting_down".to_string()
            } else {
                "running".to_string()
            };
            status.insert(name.clone(), job_status);
        }

        status
    }

    /// Stop a specific job
    pub async fn stop_job(&self, job_name: &str) -> Result<()> {
        let mut jobs = self.jobs.write().await;

        if let Some(job_handle) = jobs.remove(job_name) {
            info!("Stopping job: {}", job_name);
            job_handle.cancellation_token.cancel();

            // Wait for the job to finish
            if let Err(e) = job_handle.task_handle.await {
                error!("Error waiting for job {} to finish: {}", job_name, e);
            }

            info!("Job stopped: {}", job_name);
            Ok(())
        } else {
            warn!("Job not found: {}", job_name);
            Err(anyhow::anyhow!("Job not found: {}", job_name))
        }
    }

    /// Shutdown all jobs gracefully
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down job server");

        // Signal all jobs to stop
        self.shutdown_token.cancel();

        // Wait for all jobs to finish
        let mut jobs = self.jobs.write().await;
        let job_handles: Vec<_> = jobs.drain().collect();

        for (name, job_handle) in job_handles {
            info!("Waiting for job {} to finish", name);
            if let Err(e) = job_handle.task_handle.await {
                error!("Error waiting for job {} to finish: {}", name, e);
            }
        }

        info!("Job server shutdown complete");
        Ok(())
    }
}

impl Default for JobServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to create job configuration
pub fn create_job_config(
    name: impl Into<String>,
    interval: Duration,
    retry_count: u32,
    retry_backoff: Duration,
) -> JobConfig {
    JobConfig {
        name: name.into(),
        interval,
        retry_count,
        retry_backoff,
    }
}
