use std::path::Path;
use async_std::sync::Arc;
use tokio::sync::RwLock;
use teloxide::prelude::*;
use async_trait::async_trait;
use teloxide::types::{InputFile, MessageId, Recipient};
use once_cell::sync::Lazy;

static CHAT_ID: Lazy<Recipient> = Lazy::new(|| {
	Recipient::Id(ChatId(5173199735))
});

pub struct TelegramNotifier {
	pub bot: Arc<RwLock<Bot>>,

}

impl TelegramNotifier {
	pub fn new() -> Self {
		let bot  = Arc::new(RwLock::new(Bot::from_env()));
		
		Self {
			bot,
		}
	}
}

pub struct Notification {
	pub message: String,
	pub message_sent: bool,
	pub attachment: Option<String>,
}

#[async_trait]
pub trait Notifier {
	async fn notify(&self, notification: Notification);
}

#[async_trait]
impl Notifier for TelegramNotifier {
	async fn notify(&self, notification: Notification) {
		let bot = self.bot.read().await;
		let message = notification.message;
		let message_sent = notification.message_sent;
		println!("Message: {}", message);
		bot.send_message(CHAT_ID.clone(), message)
				.parse_mode(teloxide::types::ParseMode::MarkdownV2)
				.send()
				.await
			    .unwrap();
		if let Some(attachment) = notification.attachment {
			let inp_file = InputFile::file(Path::new(&attachment));
			
			bot.send_photo(CHAT_ID.clone(), inp_file).send().await.unwrap();
		}
	}
}