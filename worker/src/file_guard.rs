use std::path::PathBuf;

pub struct FileGuard {
    paths: Vec<PathBuf>,
}

impl FileGuard {
    pub fn new(path: PathBuf) -> Self {
        Self { paths: vec![path] }
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

pub struct PrefixGuard {
    pub dir: String,
    pub prefix: String,
    pub active: bool,
}

impl PrefixGuard {
    pub fn new(dir: String, prefix: String) -> Self {
        Self { dir, prefix, active: true }
    }
    
    pub fn cancel(mut self) {
        self.active = false;
    }
}

impl Drop for PrefixGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let dir = self.dir.clone();
        let prefix = self.prefix.clone();
        tokio::task::spawn_blocking(move || {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with(&prefix) {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        });
    }
}
