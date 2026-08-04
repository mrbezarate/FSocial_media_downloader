use tokio::process::Command;

#[tokio::main]
async fn main() {
    let mut cmd = Command::new("ls");
    cmd.process_group(0);
}
