use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use tracing_subscriber::Layer;

lazy_static::lazy_static! {
    pub static ref LOG_BUFFER: Arc<RwLock<VecDeque<String>>> = Arc::new(RwLock::new(VecDeque::with_capacity(100)));
}

pub struct MemoryLogLayer;

impl<S: tracing::Subscriber> Layer<S> for MemoryLogLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut visitor = StringVisitor(String::new());
        event.record(&mut visitor);
        let meta = event.metadata();
        
        // Very basic formatting
        let log_line = format!("[{}] {}: {}", meta.level(), meta.target(), visitor.0.trim());
        
        if let Ok(mut buf) = LOG_BUFFER.write() {
            if buf.len() >= 100 {
                buf.pop_front();
            }
            buf.push_back(log_line);
        }
    }
}

struct StringVisitor(String);
impl tracing::field::Visit for StringVisitor {
    fn record_debug(&mut self, _field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, "{:?} ", value);
    }
}
