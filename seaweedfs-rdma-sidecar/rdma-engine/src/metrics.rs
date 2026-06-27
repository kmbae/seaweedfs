use parking_lot::Mutex;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, info, warn};

#[derive(Debug)]
pub struct Metrics {
    start: Instant,
    counters: Mutex<BTreeMap<String, u64>>,
}

#[derive(Serialize)]
struct MetricsResponse {
    counters: Vec<CounterValue>,
}

#[derive(Serialize)]
struct CounterValue {
    name: String,
    value: u64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            counters: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn inc(&self, name: &str) {
        self.add(name, 1);
    }

    pub fn add(&self, name: &str, delta: u64) {
        if name.is_empty() || delta == 0 {
            return;
        }
        let mut counters = self.counters.lock();
        *counters.entry(name.to_string()).or_insert(0) += delta;
    }

    pub fn observe(&self, name: &str, duration: Duration) {
        if name.is_empty() {
            return;
        }
        self.inc(&format!("{}_ops", name));
        self.add(&format!("{}_ns", name), duration.as_nanos() as u64);
    }

    fn snapshot(&self) -> Vec<CounterValue> {
        let mut out = Vec::new();
        out.push(CounterValue {
            name: "uptime_seconds".to_string(),
            value: self.start.elapsed().as_secs(),
        });
        let counters = self.counters.lock();
        out.extend(counters.iter().map(|(name, value)| CounterValue {
            name: name.clone(),
            value: *value,
        }));
        out
    }

    pub fn response_body(&self) -> String {
        serde_json::to_string(&MetricsResponse {
            counters: self.snapshot(),
        })
        .unwrap_or_else(|_| "{\"counters\":[]}".to_string())
    }
}

pub fn metrics_addr_from_env() -> Option<SocketAddr> {
    let raw =
        std::env::var("RDMA_ENGINE_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:18085".to_string());
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || matches!(
            trimmed.to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off" | "disabled"
        )
    {
        return None;
    }
    match trimmed.parse::<SocketAddr>() {
        Ok(addr) => Some(addr),
        Err(err) => {
            warn!(
                value = trimmed,
                error = %err,
                "invalid RDMA_ENGINE_METRICS_ADDR; metrics endpoint disabled"
            );
            None
        }
    }
}

pub async fn serve(metrics: Arc<Metrics>, addr: SocketAddr) {
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            warn!(%addr, error = %err, "failed to bind RDMA engine metrics endpoint");
            return;
        }
    };
    info!("📈 RDMA engine metrics endpoint listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let metrics = metrics.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_metrics_connection(stream, metrics).await {
                        debug!(%peer, error = %err, "metrics connection failed");
                    }
                });
            }
            Err(err) => {
                warn!(error = %err, "metrics accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn handle_metrics_connection(
    mut stream: TcpStream,
    metrics: Arc<Metrics>,
) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let request = String::from_utf8_lossy(&buf[..n]);
    let status = if request.starts_with("GET /metrics ")
        || request.starts_with("GET /metrics?")
        || request.starts_with("GET / ")
    {
        "HTTP/1.1 200 OK"
    } else {
        "HTTP/1.1 404 Not Found"
    };
    let body = if status.contains("200") {
        metrics.response_body()
    } else {
        "{\"error\":\"not found\"}".to_string()
    };
    let response = format!(
        "{status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_body_includes_sorted_counters() {
        let metrics = Metrics::new();
        metrics.inc("z_counter");
        metrics.add("a_bytes", 7);
        let body = metrics.response_body();
        assert!(body.contains("\"name\":\"a_bytes\""));
        assert!(body.contains("\"value\":7"));
        assert!(body.contains("\"name\":\"z_counter\""));
        assert!(body.contains("\"name\":\"uptime_seconds\""));
    }
}
