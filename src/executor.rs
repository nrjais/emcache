use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[async_trait::async_trait]
pub trait Task: Send + Sync {
    fn name(&self) -> &str;

    async fn execute(&self) -> Result<()>;

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub name: String,
    pub interval: Duration,
    pub retry_count: u32,
    pub retry_backoff: Duration,
}

#[derive(Debug)]
struct TaskHandle {
    config: TaskConfig,
    cancellation_token: CancellationToken,
    task_handle: tokio::task::JoinHandle<()>,
}

pub struct TaskServer {
    tasks: Arc<RwLock<HashMap<String, TaskHandle>>>,
    shutdown_token: CancellationToken,
}

impl TaskServer {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            shutdown_token: CancellationToken::new(),
        }
    }

    pub async fn register<J>(&self, task: J, config: TaskConfig) -> Result<()>
    where
        J: Task + 'static,
    {
        let task_name = config.name.clone();
        info!("Registering task: {}", task_name);

        let task = Arc::new(task);
        let cancellation_token = self.shutdown_token.child_token();
        let tasks_map = Arc::clone(&self.tasks);

        let task_handle = tokio::spawn(Self::run_task_loop(task, config.clone(), cancellation_token.clone()));

        let task_handle = TaskHandle {
            config,
            cancellation_token,
            task_handle,
        };

        // Store the task handle
        let mut tasks = tasks_map.write().await;
        tasks.insert(task_name.clone(), task_handle);

        info!("Task registered successfully: {}", task_name);
        Ok(())
    }

    async fn run_task_loop<J>(task: Arc<J>, config: TaskConfig, cancellation_token: CancellationToken)
    where
        J: Task + 'static,
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

                    if let Err(e) = Self::execute_task_with_retry(&task, &config).await {
                        error!("Job {} failed after retries: {}", config.name, e);
                    }
                }
            }
        }

        info!("Shutting down job: {}", config.name);
        if let Err(e) = task.shutdown().await {
            error!("Error during job shutdown {}: {}", config.name, e);
        }

        info!("Job {} shutdown complete", config.name);
    }

    async fn execute_task_with_retry<J>(task: &Arc<J>, config: &TaskConfig) -> Result<()>
    where
        J: Task,
    {
        let mut attempts = 0;
        let mut backoff = config.retry_backoff;

        loop {
            match task.execute().await {
                Ok(()) => {
                    if attempts > 0 {
                        info!("Job {} succeeded after {} retries", config.name, attempts);
                    }
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;
                    if attempts > config.retry_count {
                        error!("Job {} failed after {} attempts: {}", config.name, attempts, e);
                        return Err(e);
                    }

                    warn!(
                        "Job {} failed (attempt {}/{}): {}. Retrying in {:?}",
                        config.name, attempts, config.retry_count, e, backoff
                    );

                    sleep(backoff).await;
                    backoff = backoff.mul_f32(1.5).min(Duration::from_secs(300));
                }
            }
        }
    }

    pub async fn stop_task(&self, task_name: &str) -> Result<()> {
        let mut tasks = self.tasks.write().await;

        if let Some(task_handle) = tasks.remove(task_name) {
            info!("Stopping task: {}", task_name);
            task_handle.cancellation_token.cancel();

            if let Err(e) = task_handle.task_handle.await {
                error!("Error waiting for task to finish: {}", e);
            }

            info!("Task stopped: {}", task_name);
            Ok(())
        } else {
            warn!("Task not found: {}", task_name);
            Err(anyhow::anyhow!("Task not found: {}", task_name))
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down task server");

        self.shutdown_token.cancel();

        let mut tasks = self.tasks.write().await;
        let task_handles: Vec<_> = tasks.drain().collect();

        for (name, task_handle) in task_handles {
            info!("Waiting for task {} to finish", name);
            if let Err(e) = task_handle.task_handle.await {
                error!("Error waiting for task {} to finish: {}", name, e);
            }
        }

        info!("Task server shutdown complete");
        Ok(())
    }
}

pub fn create_task_config(
    name: impl Into<String>,
    interval: Duration,
    retry_count: u32,
    retry_backoff: Duration,
) -> TaskConfig {
    TaskConfig {
        name: name.into(),
        interval,
        retry_count,
        retry_backoff,
    }
}
