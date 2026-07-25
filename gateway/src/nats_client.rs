use async_nats::jetstream::{self, Context, stream::Config};
use fsocial_common::subjects;
use fsocial_common::{AppError, DownloadTask};

#[derive(Clone)]
pub struct NatsClient {
    pub client: async_nats::Client,
    pub jetstream: Context,
}

impl NatsClient {
    pub async fn connect(url: &str) -> Result<Self, AppError> {
        let client = async_nats::connect(url)
            .await
            .map_err(|e| AppError::Nats(e.to_string()))?;
        let jetstream = jetstream::new(client.clone());
        Ok(Self { client, jetstream })
    }

    pub async fn setup_stream(&self) -> Result<(), AppError> {
        self.jetstream
            .get_or_create_stream(Config {
                name: subjects::STREAM_NAME.to_string(),
                subjects: vec![subjects::DOWNLOAD_TASKS_WILDCARD.to_string()],
                retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                max_age: std::time::Duration::from_secs(7200),
                ..Default::default()
            })
            .await
            .map_err(|e| AppError::Nats(e.to_string()))?;
        Ok(())
    }

    pub async fn publish_task(&self, task: &DownloadTask) -> Result<(), AppError> {
        let payload = serde_json::to_vec(task).map_err(|e| AppError::Nats(e.to_string()))?;
        let subject = if task.is_premium {
            subjects::DOWNLOAD_TASKS_PREMIUM.to_string()
        } else {
            subjects::DOWNLOAD_TASKS_FREE.to_string()
        };
        self.jetstream
            .publish(subject, payload.into())
            .await
            .map_err(|e| AppError::Nats(e.to_string()))?;
        Ok(())
    }

    pub async fn subscribe_results(&self) -> Result<async_nats::Subscriber, AppError> {
        self.client
            .subscribe(subjects::TASK_RESULTS.to_string())
            .await
            .map_err(|e| AppError::Nats(e.to_string()))
    }

    pub async fn subscribe_progress(&self) -> Result<async_nats::Subscriber, AppError> {
        self.client
            .subscribe(subjects::TASK_PROGRESS.to_string())
            .await
            .map_err(|e| AppError::Nats(e.to_string()))
    }

    pub async fn request_info(&self, req: &fsocial_common::InfoRequest) -> Result<fsocial_common::InfoResponse, AppError> {
        let payload = serde_json::to_vec(req).map_err(|e| AppError::Nats(e.to_string()))?;
        let req_future = self.client.request(subjects::INFO_REQUEST.to_string(), payload.into());
        let reply = tokio::time::timeout(std::time::Duration::from_secs(30), req_future)
            .await
            .map_err(|_| AppError::Nats("Request timed out after 30 seconds".into()))?
            .map_err(|e| AppError::Nats(e.to_string()))?;
        let info_res: fsocial_common::InfoResponse = serde_json::from_slice(&reply.payload).map_err(|e| AppError::Nats(e.to_string()))?;
        if let Some(err_msg) = info_res.error {
            return Err(AppError::YtDlp { message: err_msg, exit_code: -1 });
        }
        Ok(info_res)
    }

    pub async fn publish_command(&self, cmd: &fsocial_common::TaskCommand) -> Result<(), AppError> {
        let payload = serde_json::to_vec(cmd).map_err(|e| AppError::Nats(e.to_string()))?;
        self.client.publish(fsocial_common::subjects::TASK_COMMANDS.to_string(), payload.into())
            .await
            .map_err(|e| AppError::Nats(e.to_string()))
    }
}
