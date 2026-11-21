//! Parallel memory-mapped CSV scans. Files are cut into chunks; each worker
//! claims the next chunk and parses complete lines only.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use memmap2::{Mmap, MmapOptions};

pub const CHUNK: usize = 2 << 20;

pub fn csv_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if path.is_file()
            && name.ends_with(".csv")
            && !name.starts_with('.')
            && !name.starts_with('_')
        {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

struct Mapped {
    path: PathBuf,
    len: usize,
    map: OnceLock<io::Result<Mmap>>,
}

pub fn for_chunks<S, I, F>(paths: &[PathBuf], threads: usize, init: I, work: F) -> io::Result<Vec<S>>
where
    S: Send,
    I: Fn() -> S + Sync,
    F: Fn(&mut S, &[u8], usize, usize) + Sync,
{
    let mut files = Vec::with_capacity(paths.len());
    let mut tasks = Vec::new();
    for path in paths {
        let len = std::fs::metadata(path)?.len() as usize;
        if len == 0 {
            File::open(path)?;
            continue;
        }
        let mut start = 0;
        while start < len {
            tasks.push((files.len(), start, (start + CHUNK).min(len)));
            start += CHUNK;
        }
        files.push(Mapped { path: path.clone(), len, map: OnceLock::new() });
    }
    let next = AtomicUsize::new(0);
    let failed = Mutex::new(None);
    let states = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads.max(1))
            .map(|_| {
                scope.spawn(|| {
                    let mut state = init();
                    loop {
                        let t = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&(fi, start, end)) = tasks.get(t) else {
                            break;
                        };
                        let file = &files[fi];
                        match file.map.get_or_init(|| {
                            File::open(&file.path).and_then(|f| unsafe { MmapOptions::new().map(&f) })
                        }) {
                            Ok(map) if map.len() == file.len => work(&mut state, map, start, end),
                            Ok(map) => {
                                let msg = format!(
                                    "{}: size changed {} -> {}",
                                    file.path.display(),
                                    file.len,
                                    map.len()
                                );
                                failed.lock().unwrap().get_or_insert(io::Error::other(msg));
                                break;
                            }
                            Err(err) => {
                                failed.lock().unwrap().get_or_insert(io::Error::new(
                                    err.kind(),
                                    format!("{}: {err}", file.path.display()),
                                ));
                                break;
                            }
                        }
                    }
                    state
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    if let Some(err) = failed.into_inner().unwrap() {
        return Err(err);
    }
    // Leave maps mapped; the process exits right after writing output.
    std::mem::forget(files);
    Ok(states)
}
