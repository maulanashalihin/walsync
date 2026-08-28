use crate::proto::wal_sync_client::WalSyncClient;
use crate::proto::{Snapshot, WalChunk};
use crate::util::{self, ensure_port, file_mod_time, file_size, split_csv, wal_salt};
use log::{error, info, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::codec::CompressionEncoding;

/// Connection to a replica with reconnect support
pub struct ReplicaConn {
    addr: String,
    inner: Mutex<Option<WalSyncClient<Channel>>>,
}

impl ReplicaConn {
    pub async fn new(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let addr = ensure_port(addr);
        let channel = Channel::from_shared(format!("http://{}", addr))?
            .keep_alive_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .connect()
            .await?;
        let client = WalSyncClient::new(channel)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);
        Ok(Self {
            addr,
            inner: Mutex::new(Some(client)),
        })
    }

    pub async fn reconnect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut guard = self.inner.lock().await;
        let channel = Channel::from_shared(format!("http://{}", self.addr))?
            .keep_alive_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .connect()
            .await?;
        let client = WalSyncClient::new(channel)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);
        *guard = Some(client);
        Ok(())
    }

    pub async fn ship_wal(&self, chunk: WalChunk) -> Result<(), String> {
        let client = {
            let guard = self.inner.lock().await;
            guard.as_ref().cloned()
        };
        if let Some(mut cli) = client {
            match cli.ship_wal(chunk.clone()).await {
                Ok(resp) => {
                    let ack = resp.into_inner();
                    if ack.ok { Ok(()) } else { Err(format!("replica returned error: {}", ack.error)) }
                }
                Err(e) => Err(format!("gRPC error: {}", e)),
            }
        } else {
            Err("no connection".to_string())
        }
    }

    pub async fn ship_snapshot(&self, snap: Snapshot) -> Result<(), String> {
        let client = {
            let guard = self.inner.lock().await;
            guard.as_ref().cloned()
        };
        if let Some(mut cli) = client {
            match cli.ship_snapshot(snap.clone()).await {
                Ok(resp) => {
                    let ack = resp.into_inner();
                    if ack.ok { Ok(()) } else { Err(format!("replica returned error: {}", ack.error)) }
                }
                Err(e) => Err(format!("gRPC error: {}", e)),
            }
        } else {
            Err("no connection".to_string())
        }
    }
}

pub async fn run_primary(db_path: &str, replicas_csv: &str) {
    let replica_addrs = split_csv(replicas_csv);
    if replica_addrs.is_empty() {
        error!("primary mode requires at least one replica");
        std::process::exit(1);
    }

    info!(
        "walsync primary starting | db={} | replicas={:?}",
        db_path, replica_addrs
    );

    let wal_path = format!("{}-wal", db_path);

    // Connect to all replicas
    let mut conns: Vec<Arc<ReplicaConn>> = Vec::new();
    for addr in &replica_addrs {
        match ReplicaConn::new(addr).await {
            Ok(rc) => {
                info!("connected to replica {}", ensure_port(addr));
                conns.push(Arc::new(rc));
            }
            Err(e) => {
                error!("failed to connect to replica {}: {}", addr, e);
                std::process::exit(1);
            }
        }
    }

    // Ship initial snapshot
    info!("shipping initial snapshot...");
    ship_snapshot_grpc(db_path, &wal_path, &conns).await;
    info!("initial snapshot shipped");

    // Track state
    let mut last_shipped_size = file_size(&wal_path);
    let mut last_shipped_db_mod = file_mod_time(db_path);
    let (mut last_salt1, mut last_salt2) = wal_salt(&wal_path);

    // File watcher
    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(16);
    let db_path_w = db_path.to_string();
    let wal_path_w = wal_path.clone();
    let tx_clone = tx.clone();

    tokio::spawn(async move {
        use notify::{EventKind, RecursiveMode, Watcher};
        let mut watcher = match notify::recommended_watcher(move |res: Result<notify::Event, _>| {
            if let Ok(event) = res {
                let _ = tx_clone.blocking_send(());
                // suppress unused warning
                let _ = &event;
            }
        }) {
            Ok(w) => w,
            Err(e) => {
                error!("failed to create file watcher: {}", e);
                return;
            }
        };

        let dir = std::path::Path::new(&db_path_w)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        if let Err(e) = watcher.watch(dir, RecursiveMode::NonRecursive) {
            error!("failed to watch directory {}: {}", dir.display(), e);
        }

        // Keep watcher alive forever
        std::future::pending::<()>().await;
        let _ = &wal_path_w;
    });

    // Polling fallback + debounce
    let mut ticker = tokio::time::interval(Duration::from_millis(50));
    let mut debounce_active = false;
    let mut debounce_rx: Option<tokio::time::Sleep> = None;

    loop {
        // Poll for changes
        ticker.tick().await;

        let cur_wal_size = file_size(&wal_path);

        // Schedule debounce if WAL grew
        if cur_wal_size > last_shipped_size {
            if !debounce_active {
                debounce_active = true;
                debounce_rx = Some(tokio::time::sleep(Duration::from_millis(50)));
            }
        }

        // Check for checkpoint (DB file modified)
        let cur_db_mod = file_mod_time(db_path);
        if cur_db_mod != last_shipped_db_mod {
            info!("checkpoint detected, shipping snapshot...");
            ship_snapshot_grpc(db_path, &wal_path, &conns).await;
            last_shipped_size = file_size(&wal_path);
            last_shipped_db_mod = cur_db_mod;
            let (s1, s2) = wal_salt(&wal_path);
            last_salt1 = s1;
            last_salt2 = s2;
            debounce_active = false;
            debounce_rx = None;
            continue;
        }

        // Check WAL salt change
        let (cur_salt1, cur_salt2) = wal_salt(&wal_path);
        if cur_salt1 != last_salt1 || cur_salt2 != last_salt2 {
            if cur_salt1 != 0 {
                info!("WAL salt changed, shipping snapshot...");
                ship_snapshot_grpc(db_path, &wal_path, &conns).await;
                last_shipped_size = file_size(&wal_path);
                last_salt1 = cur_salt1;
                last_salt2 = cur_salt2;
                debounce_active = false;
                debounce_rx = None;
                continue;
            }
        }

        // Fire debounce if active and elapsed
        if debounce_active {
            if let Some(mut t) = debounce_rx.take() {
                tokio::pin!(t);
                tokio::select! {
                    _ = &mut t => {
                        debounce_active = false;
                        let cur = file_size(&wal_path);
                        if cur > last_shipped_size {
                            ship_wal_grpc(&wal_path, last_shipped_size, cur, &conns).await;
                            last_shipped_size = cur;
                        }
                    }
                    _ = ticker.tick() => {
                        // Not yet elapsed — put it back
                        // t is pinned, can't move out. Recreate instead.
                        debounce_rx = Some(tokio::time::sleep(Duration::from_millis(50)));
                    }
                }
            }
        }
    }
}

async fn ship_snapshot_grpc(db_path: &str, wal_path: &str, conns: &[Arc<ReplicaConn>]) {
    let db_data = match std::fs::read(db_path) {
        Ok(d) => d,
        Err(e) => {
            error!("error reading db file: {}", e);
            return;
        }
    };

    let wal_data = std::fs::read(wal_path).unwrap_or_default();
    let db_len = db_data.len();
    let wal_len = wal_data.len();

    let mut handles = Vec::new();
    for rc in conns {
        let rc = rc.clone();
        let db_clone = db_data.clone();
        let wal_clone = wal_data.clone();
        handles.push(tokio::spawn(async move {
            let snap = Snapshot {
                db_data: db_clone.clone(),
                wal_data: wal_clone.clone(),
            };
            match rc.ship_snapshot(snap).await {
                Ok(()) => {
                    info!(
                        "snapshot shipped to {} ({} bytes db, {} bytes wal)",
                        rc.addr, db_len, wal_len
                    );
                }
                Err(e) => {
                    warn!("snapshot to {} failed: {}, reconnecting...", rc.addr, e);
                    if let Err(re) = rc.reconnect().await {
                        warn!("reconnect to {} failed: {}", rc.addr, re);
                        return;
                    }
                    // Retry once
                    let snap2 = Snapshot {
                        db_data: db_clone.clone(),
                        wal_data: wal_clone.clone(),
                    };
                    if let Err(e2) = rc.ship_snapshot(snap2).await {
                        warn!("retry snapshot to {} failed: {}", rc.addr, e2);
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    info!("snapshot shipped: {} bytes db, {} bytes wal", db_len, wal_len);
}

async fn ship_wal_grpc(wal_path: &str, offset: i64, size: i64, conns: &[Arc<ReplicaConn>]) {
    let data = match util::read_file_range(wal_path, offset as u64, (size - offset) as u64) {
        Ok(d) => d,
        Err(e) => {
            error!("error reading wal data: {}", e);
            return;
        }
    };

    if data.is_empty() {
        return;
    }

    let data_len = data.len();
    let n_conns = conns.len();

    let mut handles = Vec::new();
    for rc in conns {
        let rc = rc.clone();
        let data_clone = data.clone();
        handles.push(tokio::spawn(async move {
            let chunk = WalChunk {
                offset,
                data: data_clone.clone(),
            };
            match rc.ship_wal(chunk).await {
                Ok(()) => {}
                Err(e) => {
                    warn!("wal ship to {} failed: {}, reconnecting...", rc.addr, e);
                    if let Err(re) = rc.reconnect().await {
                        warn!("reconnect to {} failed: {}", rc.addr, re);
                        return;
                    }
                    let chunk2 = WalChunk {
                        offset,
                        data: data_clone.clone(),
                    };
                    if let Err(e2) = rc.ship_wal(chunk2).await {
                        warn!("retry wal ship to {} failed: {}", rc.addr, e2);
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    info!(
        "WAL shipped: {} bytes from offset {} to {} replicas",
        data_len, offset, n_conns
    );
}
