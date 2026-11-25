use std::path::PathBuf;

use anyhow::{Context, Result};
use directories::ProjectDirs;

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub data_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub db_path: PathBuf,
    pub socket_path: PathBuf,
}

impl DaemonConfig {
    /// Resolves XDG directories and runtime socket path for `pelliper-daemon`.
    pub fn load() -> Result<Self> {
        let proj = ProjectDirs::from("com", "pelliper", "pelliper")
            .context("unable to resolve XDG project directories")?;

        let data_dir = proj.data_dir().to_path_buf();
        let cache_dir = proj.cache_dir().to_path_buf();
        let db_path = data_dir.join("pelliper.db");

        let runtime_base = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let socket_path = runtime_base.join("pelliper").join("daemon.sock");

        Ok(Self {
            data_dir,
            cache_dir,
            db_path,
            socket_path,
        })
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        std::fs::create_dir_all(&self.data_dir)?;
        std::fs::create_dir_all(self.cache_dir.join("attachments"))?;
        if let Some(parent) = self.socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }
}
