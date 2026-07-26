use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tracing::{info, warn};

pub struct ProxyEntry {
    pub url: String,
    pub cooldown_until: RwLock<Option<Instant>>,
    pub failures: RwLock<u32>,
}

pub struct ProxyPool {
    proxies: Vec<ProxyEntry>,
    current: AtomicUsize,
}

impl ProxyPool {
    pub fn new(proxy_urls: Vec<String>) -> Self {
        let proxies = proxy_urls
            .into_iter()
            .map(|url| ProxyEntry {
                url,
                cooldown_until: RwLock::new(None),
                failures: RwLock::new(0),
            })
            .collect();

        Self {
            proxies,
            current: AtomicUsize::new(0),
        }
    }

    pub fn next(&self) -> Option<&str> {
        if self.proxies.is_empty() {
            return None;
        }

        let now = Instant::now();
        let start = self.current.load(Ordering::Relaxed);
        let len = self.proxies.len();

        for i in 0..len {
            let idx = (start + i) % len;
            let proxy = &self.proxies[idx];

            let on_cooldown = {
                if let Some(cooldown) = *proxy.cooldown_until.read().unwrap() {
                    cooldown > now
                } else {
                    false
                }
            };

            if !on_cooldown {
                self.current.store((idx + 1) % len, Ordering::Relaxed);
                return Some(&proxy.url);
            }
        }

        None
    }

    pub fn mark_failed(&self, proxy_url: &str) {
        if let Some(proxy) = self.proxies.iter().find(|p| p.url == proxy_url) {
            let mut fails = proxy.failures.write().unwrap();
            *fails += 1;

            let backoff_secs = 30 * (2_u64.pow((*fails - 1).min(6))); // max ~32 mins
            let cooldown = Instant::now() + Duration::from_secs(backoff_secs);

            *proxy.cooldown_until.write().unwrap() = Some(cooldown);
            warn!(
                "Proxy {} marked as failed (fail count: {}). Cooldown for {}s",
                proxy_url, *fails, backoff_secs
            );
        }
    }

    pub fn mark_success(&self, proxy_url: &str) {
        if let Some(proxy) = self.proxies.iter().find(|p| p.url == proxy_url) {
            let mut fails = proxy.failures.write().unwrap();
            if *fails > 0 {
                info!("Proxy {} recovered.", proxy_url);
                *fails = 0;
            }
            *proxy.cooldown_until.write().unwrap() = None;
        }
    }
}
