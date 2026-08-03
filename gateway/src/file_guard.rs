use std::path::PathBuf;

pub struct FileGuard {
    paths: Vec<PathBuf>,
}

impl FileGuard {
    pub fn new(path: PathBuf) -> Self {
        Self { paths: vec![path] }
    }
    
    pub fn empty() -> Self {
        Self { paths: Vec::new() }
    }
    
    pub fn add(&mut self, path: PathBuf) {
        self.paths.push(path);
    }
    
    pub fn cancel(mut self) {
        self.paths.clear();
    }
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        if self.paths.is_empty() {
            return;
        }
        let paths = self.paths.clone();
        tokio::task::spawn_blocking(move || {
            for p in paths {
                if p.exists() {
                    let _ = std::fs::remove_file(p);
                }
            }
        });
    }
}
