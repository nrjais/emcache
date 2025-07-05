use anyhow::Result;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub trait Task: Sync + Send {
    fn name(&self) -> String;

    fn execute(&self, cancellation_token: CancellationToken) -> impl Future<Output = Result<()>> + Send;

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct TaskHandle {
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

    pub async fn register<J>(&self, task: J) -> Result<()>
    where
        J: Task + 'static,
    {
        let task_name = task.name();
        info!("Registering task: {}", &task_name);

        let task = Arc::new(task);
        let cancellation_token = self.shutdown_token.child_token();
        let tasks_map = Arc::clone(&self.tasks);

        let task_handle = tokio::spawn(Self::run_task_loop(task, cancellation_token.clone()));

        let task_handle = TaskHandle {
            cancellation_token,
            task_handle,
        };

        let mut tasks = tasks_map.write().await;
        tasks.insert(task_name.clone(), task_handle);

        info!("Task registered successfully: {}", task_name);
        Ok(())
    }

    async fn run_task_loop<J>(task: Arc<J>, cancellation_token: CancellationToken)
    where
        J: Task + 'static,
    {
        let res = task.execute(cancellation_token).await;
        if let Err(e) = res {
            error!("Job {} failed: {}", task.name(), e);
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
