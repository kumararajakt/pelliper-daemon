use std::env;
use std::fs;
use std::path::Path;

const ALLOWED_KEYS: &[&str] = &[
    "PELLIPER_GOOGLE_CLIENT_ID",
    "PELLIPER_GOOGLE_CLIENT_SECRET",
    "PELLIPER_MS_CLIENT_ID",
    "PELLIPER_FASTMAIL_CLIENT_ID",
];

fn load_env_file(path: &Path) {
    let Ok(contents) = fs::read_to_string(path) else {
        return;
    };
    for raw_line in contents.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim();
        let Some((key, val)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if !ALLOWED_KEYS.contains(&key) {
            continue;
        }
        if env::var_os(key).is_some() {
            continue;
        }
        let val = val.trim();
        let val = if (val.starts_with('"') && val.ends_with('"') && val.len() >= 2)
            || (val.starts_with('\'') && val.ends_with('\'') && val.len() >= 2)
        {
            &val[1..val.len() - 1]
        } else {
            val
        };
        if !val.is_empty() {
            println!("cargo:rustc-env={key}={val}");
        }
    }
}

fn main() {
    println!("cargo:rerun-if-changed=.env");
    println!("cargo:rerun-if-changed=../.env");
    for key in ALLOWED_KEYS {
        println!("cargo:rerun-if-env-changed={key}");
    }

    // Load daemon/.env first, then project-root ../.env (process env always wins)
    load_env_file(Path::new(".env"));
    load_env_file(Path::new("../.env"));
}
