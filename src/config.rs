use crate::Config;
use log::info;
use std::fs;

pub fn load_config(path: &str) -> Config {
    let mut cfg = Config::default();

    let data = match fs::read_to_string(path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error reading config file {}: {}", path, e);
            std::process::exit(1);
        }
    };

    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(eq) = line.find('=') {
            let key = line[..eq].trim();
            let mut val = line[eq + 1..].trim();
            // Strip quotes
            if val.len() >= 2 && val.starts_with('"') && val.ends_with('"') {
                val = &val[1..val.len() - 1];
            }
            match key {
                "mode" => cfg.mode = val.to_string(),
                "db" => cfg.db = val.to_string(),
                "replicas" => cfg.replicas = val.to_string(),
                "listen" => cfg.listen = val.to_string(),
                "metrics" => cfg.metrics = val.to_string(),
                _ => {}
            }
        }
    }

    info!("config loaded from {}", path);
    cfg
}
