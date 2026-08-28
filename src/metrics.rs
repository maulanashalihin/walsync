use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use log::info;
use std::convert::Infallible;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Default)]
pub struct Metrics {
    pub wal_ships: AtomicI64,
    pub wal_bytes: AtomicI64,
    pub wal_errors: AtomicI64,
    pub snapshots: AtomicI64,
    pub snapshot_bytes: AtomicI64,
    pub snapshot_errors: AtomicI64,
    pub last_ship_unix: AtomicI64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inc_wal_ships(&self, bytes: i64) {
        self.wal_ships.fetch_add(1, Ordering::Relaxed);
        self.wal_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.last_ship_unix
            .store(chrono_like_now(), Ordering::Relaxed);
    }

    pub fn inc_wal_error(&self) {
        self.wal_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_snapshot(&self, bytes: i64) {
        self.snapshots.fetch_add(1, Ordering::Relaxed);
        self.snapshot_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.last_ship_unix
            .store(chrono_like_now(), Ordering::Relaxed);
    }

    pub fn inc_snapshot_error(&self) {
        self.snapshot_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn render(&self) -> String {
        format!(
            "# walsync metrics\n\
             walsync_wal_ships_total {}\n\
             walsync_wal_shipped_bytes_total {}\n\
             walsync_wal_ship_errors_total {}\n\
             walsync_snapshot_ships_total {}\n\
             walsync_snapshot_shipped_bytes_total {}\n\
             walsync_snapshot_ship_errors_total {}\n\
             walsync_last_ship_timestamp_seconds {}\n",
            self.wal_ships.load(Ordering::Relaxed),
            self.wal_bytes.load(Ordering::Relaxed),
            self.wal_errors.load(Ordering::Relaxed),
            self.snapshots.load(Ordering::Relaxed),
            self.snapshot_bytes.load(Ordering::Relaxed),
            self.snapshot_errors.load(Ordering::Relaxed),
            self.last_ship_unix.load(Ordering::Relaxed),
        )
    }
}

fn chrono_like_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub async fn start_metrics_server(addr: &str) {
    let bind_addr = if addr.starts_with(':') {
        format!("0.0.0.0{}", addr)
    } else {
        addr.to_string()
    };

    let listener = match TcpListener::bind(&bind_addr).await {
        Ok(l) => l,
        Err(e) => {
            log::error!("metrics server bind error: {}", e);
            return;
        }
    };

    info!("metrics server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let io = TokioIo::new(stream);
                tokio::spawn(async move {
                    let svc = service_fn(|_req: Request<hyper::body::Incoming>| async move {
                        let body = "# walsync metrics\n\
                            walsync_wal_ships_total 0\n\
                            walsync_wal_shipped_bytes_total 0\n\
                            walsync_wal_ship_errors_total 0\n\
                            walsync_snapshot_ships_total 0\n\
                            walsync_snapshot_shipped_bytes_total 0\n\
                            walsync_snapshot_ship_errors_total 0\n\
                            walsync_last_ship_timestamp_seconds 0\n";
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("Content-Type", "text/plain; version=0.0.4")
                                .body(Full::new(Bytes::from(body)))
                                .unwrap(),
                        )
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
            Err(e) => {
                log::error!("metrics accept error: {}", e);
            }
        }
    }
}
