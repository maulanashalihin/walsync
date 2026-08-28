use crate::proto::wal_sync_server::{WalSync, WalSyncServer};
use crate::proto::{Ack, Empty, HealthResponse, Snapshot, WalChunk};
use crate::util;
use log::{error, info};
use std::pin::Pin;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub struct ReplicaServer {
    db_path: String,
    wal_path: String,
}

impl ReplicaServer {
    pub fn new(db_path: &str) -> Self {
        Self {
            db_path: db_path.to_string(),
            wal_path: format!("{}-wal", db_path),
        }
    }
}

#[tonic::async_trait]
impl WalSync for ReplicaServer {
    async fn ship_snapshot(
        &self,
        request: Request<Snapshot>,
    ) -> Result<Response<Ack>, Status> {
        let snap = request.into_inner();

        // Atomic replace: write to .tmp, remove stale WAL/SHM, rename
        if let Err(e) = util::atomic_replace_db(&self.db_path, &snap.db_data) {
            error!("snapshot write error: {}", e);
            return Ok(Response::new(Ack {
                ok: false,
                error: e.to_string(),
                applied_offset: 0,
            }));
        }

        // Write WAL if provided
        if !snap.wal_data.is_empty() {
            if let Err(e) = std::fs::write(&self.wal_path, &snap.wal_data) {
                error!("snapshot wal write error: {}", e);
                return Ok(Response::new(Ack {
                    ok: false,
                    error: e.to_string(),
                    applied_offset: 0,
                }));
            }
        }

        info!(
            "snapshot received: {} bytes db, {} bytes wal",
            snap.db_data.len(),
            snap.wal_data.len()
        );
        Ok(Response::new(Ack {
            ok: true,
            error: String::new(),
            applied_offset: 0,
        }))
    }

    async fn ship_wal(
        &self,
        request: Request<WalChunk>,
    ) -> Result<Response<Ack>, Status> {
        let chunk = request.into_inner();

        if chunk.data.is_empty() {
            return Ok(Response::new(Ack {
                ok: true,
                error: String::new(),
                applied_offset: 0,
            }));
        }

        let offset = chunk.offset;

        if let Err(e) = util::write_file_at(&self.wal_path, offset, &chunk.data) {
            error!("wal write error: {}", e);
            return Ok(Response::new(Ack {
                ok: false,
                error: e.to_string(),
                applied_offset: 0,
            }));
        }

        info!(
            "WAL received: {} bytes at offset {}",
            chunk.data.len(),
            offset
        );
        Ok(Response::new(Ack {
            ok: true,
            error: String::new(),
            applied_offset: offset + chunk.data.len() as i64,
        }))
    }

    async fn health(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            ok: true,
            db_size: util::file_size(&self.db_path),
            wal_size: util::file_size(&self.wal_path),
        }))
    }
}

pub async fn run_replica(db_path: &str, listen: &str) {
    info!("walsync replica starting | db={} | listen={}", db_path, listen);

    let bind_addr = if listen.starts_with(':') {
        format!("0.0.0.0{}", listen)
    } else {
        listen.to_string()
    };

    let addr: std::net::SocketAddr = match bind_addr.parse() {
        Ok(a) => a,
        Err(e) => {
            error!("failed to parse listen address {}: {}", bind_addr, e);
            std::process::exit(1);
        }
    };

    let server = ReplicaServer::new(db_path);

    info!("replica listening on {} (gRPC)", listen);

    if let Err(e) = Server::builder()
        .add_service(
            WalSyncServer::new(server)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .serve(addr)
        .await
    {
        error!("server error: {}", e);
        std::process::exit(1);
    }
}
