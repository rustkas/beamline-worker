use std::fs::{OpenOptions, rename, metadata, remove_file, read_dir, create_dir_all};
use std::io::Write;
use std::path::Path;
use crate::protocol::DeadLetter;
use chrono::Utc;

fn rotate_if_needed(path: &str, max_bytes: u64, max_rotations: u32, total_max_bytes: u64, max_age_days: Option<u32>) -> Result<(), std::io::Error> {
    if let Ok(meta) = metadata(path) {
        if meta.len() >= max_bytes {
            let ts = Utc::now().format("%Y%m%d-%H%M%S").to_string();
            let rotated = format!("{}.{}", path, ts);
            rename(path, &rotated)?;

            enforce_limits(path, max_rotations, total_max_bytes, max_age_days)?;
        }
    }
    Ok(())
}

fn enforce_limits(path: &str, max_rotations: u32, total_max_bytes: u64, max_age_days: Option<u32>) -> Result<(), std::io::Error> {
    let base = Path::new(path);
    let dir = base.parent().unwrap_or(Path::new("."));
    let base_name = base.file_name().unwrap().to_string_lossy().to_string();

    let mut rotated_files: Vec<(String, u64)> = Vec::new();
    for e in read_dir(dir)?.flatten() {
        if let Ok(md) = e.metadata() {
            if let Some(name) = e.file_name().to_str() {
                if name.starts_with(&format!("{}.", base_name)) {
                    rotated_files.push((dir.join(name).to_string_lossy().to_string(), md.len()));
                }
            }
        }
    }
    // Sort by filename ascending (timestamp ensures chronological order)
    rotated_files.sort_by(|a, b| a.0.cmp(&b.0));
    if let Some(days) = max_age_days {
        let now = Utc::now();
        rotated_files.retain(|(name, _)| {
            if let Ok(md) = metadata(name) {
                if let Ok(modified) = md.modified() {
                    let file_dt: chrono::DateTime<chrono::Utc> = modified.into();
                    let age = now.signed_duration_since(file_dt).num_days();
                    if age > days as i64 {
                        let _ = remove_file(name);
                        return false;
                    }
                }
            }
            true
        });
    }
    while rotated_files.len() > max_rotations as usize {
        if let Some((oldest, _)) = rotated_files.first().cloned() {
            let _ = remove_file(&oldest);
            rotated_files.remove(0);
        } else {
            break;
        }
    }
    let mut total: u64 = rotated_files.iter().map(|(_, sz)| *sz).sum();
    while total > total_max_bytes {
        if let Some((oldest, sz)) = rotated_files.first().cloned() {
            let _ = remove_file(&oldest);
            rotated_files.remove(0);
            total = total.saturating_sub(sz);
        } else {
            break;
        }
    }
    Ok(())
}

pub fn write_deadletter_to_file(dlq: &DeadLetter, path: &str, max_bytes: u64, max_rotations: u32, total_max_bytes: u64, max_age_days: Option<u32>) -> Result<(), std::io::Error> {
    if let Some(parent) = Path::new(path).parent() {
        let _ = create_dir_all(parent);
    }
    rotate_if_needed(path, max_bytes, max_rotations, total_max_bytes, max_age_days)?;
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(dlq).unwrap_or_else(|_| "{}".to_string());
    f.write_all(line.as_bytes())?;
    f.write_all(b"\n")?;
    Ok(())
}
