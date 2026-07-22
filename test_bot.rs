use teloxide::prelude::*;
use std::time::Duration;

fn main() {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()
        .unwrap();
    let bot = Bot::new("TOKEN").with_client(client);
}
