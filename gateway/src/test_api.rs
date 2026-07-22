use teloxide::prelude::*;
fn test() {
    let client = reqwest::Client::builder().timeout(std::time::Duration::from_secs(600)).build().unwrap();
    let bot = Bot::from_env().client(client);
}
