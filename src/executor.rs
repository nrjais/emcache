use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub trait Task: Sync + Send {
    fn name(&self) -> String;

    fn execute(&self) -> impl std::future::Future<Output = Result<()>> + Send;

    fn shutdown(&self) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub interval: Duration,
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
        let task_name = task.name();
        info!("Registering task: {}", &task_name);

        let task = Arc::new(task);
        let cancellation_token = self.shutdown_token.child_token();
        let tasks_map = Arc::clone(&self.tasks);

        let task_handle = tokio::spawn(Self::run_task_loop(task, config.clone(), cancellation_token.clone()));

        let task_handle = TaskHandle {
            config,
            cancellation_token,
            task_handle,
        };

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

        info!("Starting job loop for: {}", task.name());

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Job {} received shutdown signal", task.name());
                    break;
                }
                _ = interval.tick() => {
                    debug!("Executing job: {}", task.name());

                    if let Err(e) = task.execute().await {
                        error!("Job {} failed: {}", task.name(), e);
                    }
                }
            }
        }

        info!("Shutting down job: {}", task.name());
        if let Err(e) = task.shutdown().await {
            error!("Error during job shutdown {}: {}", task.name(), e);
        }

        info!("Job {} shutdown complete", task.name());
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

pub fn task_config(interval: Duration) -> TaskConfig {
    TaskConfig { interval }
}
