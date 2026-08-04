use std::process::Stdio;
use tokio::process::Command;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let mut cmd = Command::new("bash");
    cmd.arg("-c").arg("sleep 10");
    
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            Ok(())
        });
    }

    let child = cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
    let pid = child.id().unwrap();
    
    struct KillProcessGroupOnDrop(Option<u32>);
    impl Drop for KillProcessGroupOnDrop {
        fn drop(&mut self) {
            if let Some(pid) = self.0 {
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGKILL);
                }
            }
        }
    }
    
    let mut _kill_guard = KillProcessGroupOnDrop(Some(pid));
    
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("Dropping guard now...");
    drop(_kill_guard);
    
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("Done.");
}
