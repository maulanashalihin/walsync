mod config;
mod metrics;
mod primary;
mod replica;
mod util;

pub mod proto {
    tonic::include_proto!("walsync");
}

use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Clone, Debug, ValueEnum, PartialEq)]
pub enum Mode {
    Primary,
    Replica,
}

#[derive(Parser, Debug)]
#[command(name = "walsync", version, about = "Live SQLite WAL shipping replication via gRPC")]
pub struct Cli {
    /// Operation mode: primary or replica
    #[arg(long, value_enum)]
    pub mode: Option<Mode>,

    /// Path to SQLite database file
    #[arg(long)]
    pub db: Option<String>,

    /// Comma-separated replica addresses (primary mode, e.g. host:port)
    #[arg(long)]
    pub replicas: Option<String>,

    /// gRPC listen address (replica mode)
    #[arg(long, default_value = ":9090")]
    pub listen: String,

    /// HTTP metrics listen address (e.g. :9091, empty = disabled)
    #[arg(long)]
    pub metrics: Option<String>,

    /// Path to TOML config file (CLI flags override config)
    #[arg(long)]
    pub config: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct Config {
    pub mode: String,
    pub db: String,
    pub replicas: String,
    pub listen: String,
    pub metrics: String,
}

pub fn merge_config(cli: &Cli, cfg: &Config) -> (String, String, String, String, String) {
    let mode = cli.mode.as_ref().map(|m| {
        match m {
            Mode::Primary => "primary",
            Mode::Replica => "replica",
        }
        .to_string()
    }).unwrap_or_else(|| cfg.mode.clone());

    let db = cli.db.clone().unwrap_or_else(|| cfg.db.clone());
    let replicas = cli.replicas.clone().unwrap_or_else(|| cfg.replicas.clone());
    let listen = if cli.listen != ":9090" || cfg.listen.is_empty() {
        cli.listen.clone()
    } else {
        cfg.listen.clone()
    };
    let metrics = cli.metrics.clone().unwrap_or_else(|| cfg.metrics.clone());

    (mode, db, replicas, listen, metrics)
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let cli = Cli::parse();

    // Load config file if specified
    let cfg = if let Some(ref config_path) = cli.config {
        config::load_config(config_path)
    } else {
        Config::default()
    };

    let (mode, db_path, replicas, listen, metrics_addr) = merge_config(&cli, &cfg);

    if mode.is_empty() {
        eprintln!("error: -mode is required (primary or replica)");
        std::process::exit(1);
    }
    if db_path.is_empty() {
        eprintln!("error: -db is required");
        std::process::exit(1);
    }

    // Start metrics server if configured
    if !metrics_addr.is_empty() {
        let addr = metrics_addr.clone();
        tokio::spawn(async move {
            metrics::start_metrics_server(&addr).await;
        });
    }

    match mode.as_str() {
        "primary" => {
            if replicas.is_empty() {
                eprintln!("error: primary mode requires -replicas");
                std::process::exit(1);
            }
            primary::run_primary(&db_path, &replicas).await;
        }
        "replica" => {
            replica::run_replica(&db_path, &listen).await;
        }
        _ => {
            eprintln!("error: invalid mode '{}', use primary or replica", mode);
            std::process::exit(1);
        }
    }
}
