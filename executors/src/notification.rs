use async_std::sync::Arc;
use tokio::sync::RwLock;
use teloxide::prelude::*;
use async_trait::async_trait;
use teloxide::types::{MessageId, Recipient};

pub struct TelegramNotifier {
	pub bot: Arc<RwLock<Bot>>,
	pub chat_id: Recipient,
	pub message: String,
	pub message_sent: bool,
}

pub struct Notification {
	pub message: String,
	pub message_id: MessageId,
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
		let attachment = notification.attachment;
		let message_id = notification.message_id;
		let message_sent = notification.message_sent;
		if message_sent {
			bot.edit_message_text(self.chat_id.clone(), message_id, message)
				.parse_mode(teloxide::types::ParseMode::Markdown)
				.send()
				.await
				.unwrap();
		} else {
			bot.send_message(self.chat_id.clone(), message)
				.parse_mode(teloxide::types::ParseMode::Markdown)
				.send()
				.await
				.unwrap();
		}
	}
}