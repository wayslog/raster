//! Storage devices (FASTER `device/*`).
//!
//! Devices run I/O asynchronously and invoke the completion callback on an I/O
//! thread. Callbacks must only touch thread-safe state.
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

pub type WriteCallback = Box<dyn FnOnce(io::Result<()>) + Send>;
pub type ReadCallback = Box<dyn FnOnce(io::Result<Vec<u8>>) + Send>;

pub trait Device: Send + Sync + 'static {
    /// Writes `len` bytes from `data` at `offset`.
    ///
    /// # Safety
    /// `data..data + len` must stay valid and unmodified until `done` runs.
    unsafe fn write_async(&self, offset: u64, data: *const u8, len: usize, done: WriteCallback);
    fn read_async(&self, offset: u64, len: usize, done: ReadCallback);
}

/// Discards writes; reads complete with an error because nothing was stored.
pub struct NullDevice;

impl Device for NullDevice {
    unsafe fn write_async(&self, _: u64, _: *const u8, _: usize, done: WriteCallback) {
        done(Ok(()));
    }
    fn read_async(&self, _: u64, _: usize, done: ReadCallback) {
        done(Err(io::Error::new(
            io::ErrorKind::NotFound,
            "null device stores nothing",
        )));
    }
}

type Job = Box<dyn FnOnce() + Send>;

struct Queue {
    jobs: Mutex<(Vec<Job>, bool)>,
    ready: Condvar,
}

/// Log stored in fixed-size segment files, served by a pool of I/O threads.
pub struct FileDevice {
    inner: Arc<FileInner>,
    queue: Arc<Queue>,
    workers: Vec<JoinHandle<()>>,
}

struct FileInner {
    directory: PathBuf,
    direct: bool,
    segment_bits: u32,
    segments: Mutex<HashMap<u64, Arc<File>>>,
}

struct SendPtr(*const u8);
// SAFETY: the caller of `write_async` keeps the buffer alive until completion.
unsafe impl Send for SendPtr {}
impl SendPtr {
    fn get(&self) -> *const u8 {
        self.0
    }
}

/// Alignment and length granularity of unbuffered I/O.
pub const SECTOR: u64 = 512;

impl FileInner {
    fn segment(&self, index: u64) -> io::Result<Arc<File>> {
        let mut segments = self.segments.lock().unwrap();
        if let Some(file) = segments.get(&index) {
            return Ok(file.clone());
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(target_os = "linux")]
        if self.direct {
            use std::os::unix::fs::OpenOptionsExt;
            #[cfg(target_arch = "aarch64")]
            const O_DIRECT: i32 = 0o200000;
            #[cfg(not(target_arch = "aarch64"))]
            const O_DIRECT: i32 = 0o40000;
            options.custom_flags(O_DIRECT);
        }
        let file = Arc::new(options.open(self.directory.join(format!("log.{index}")))?);
        segments.insert(index, file.clone());
        Ok(file)
    }

    /// Splits `[offset, offset + len)` at segment boundaries.
    fn for_each_piece(
        &self,
        offset: u64,
        len: usize,
        mut f: impl FnMut(&File, u64, usize, usize) -> io::Result<()>,
    ) -> io::Result<()> {
        let segment_size = 1u64 << self.segment_bits;
        let mut done = 0usize;
        while done < len {
            let at = offset + done as u64;
            let in_segment = at & (segment_size - 1);
            let piece = ((segment_size - in_segment) as usize).min(len - done);
            let file = self.segment(at >> self.segment_bits)?;
            f(&file, in_segment, done, piece)?;
            done += piece;
        }
        Ok(())
    }

    fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if !self.direct {
            let mut buffer = vec![0u8; len];
            self.for_each_piece(offset, len, |file, at, from, piece| {
                file.read_exact_at(&mut buffer[from..from + piece], at)
            })?;
            return Ok(buffer);
        }
        let start = offset & !(SECTOR - 1);
        let end = (offset + len as u64).next_multiple_of(SECTOR);
        let mut aligned = AlignedBuffer::new((end - start) as usize);
        let total = aligned.len();
        self.for_each_piece(start, total, |file, at, from, piece| {
            file.read_exact_at(&mut aligned.as_mut()[from..from + piece], at)
        })?;
        let skip = (offset - start) as usize;
        Ok(aligned.as_mut()[skip..skip + len].to_vec())
    }
}

/// Sector-aligned, zero-initialized heap buffer for unbuffered I/O.
pub(crate) struct AlignedBuffer {
    ptr: *mut u8,
    layout: std::alloc::Layout,
}

impl AlignedBuffer {
    pub(crate) fn new(len: usize) -> Self {
        let layout =
            std::alloc::Layout::from_size_align(len, SECTOR as usize).expect("buffer layout");
        // SAFETY: `len` is a non-zero multiple of the sector size.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "read buffer allocation failed");
        Self { ptr, layout }
    }
    pub(crate) fn len(&self) -> usize {
        self.layout.size()
    }
    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.ptr
    }
    pub(crate) fn as_mut(&mut self) -> &mut [u8] {
        // SAFETY: owned, initialized allocation of `len` bytes.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.layout.size()) }
    }
}

// SAFETY: the buffer is uniquely owned heap memory.
unsafe impl Send for AlignedBuffer {}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: allocated in `new` with this layout.
        unsafe { std::alloc::dealloc(self.ptr, self.layout) };
    }
}

impl FileDevice {
    pub fn open(
        directory: impl Into<PathBuf>,
        segment_bits: u32,
        threads: usize,
    ) -> io::Result<Self> {
        Self::open_with(directory, segment_bits, threads, false)
    }

    /// With `direct`, bypasses the OS page cache (O_DIRECT on Linux), like
    /// FASTER's default unbuffered files. Page writes are already sector
    /// aligned; reads are widened to sector boundaries.
    pub fn open_with(
        directory: impl Into<PathBuf>,
        segment_bits: u32,
        threads: usize,
        direct: bool,
    ) -> io::Result<Self> {
        let directory = directory.into();
        std::fs::create_dir_all(&directory)?;
        let queue = Arc::new(Queue {
            jobs: Mutex::new((Vec::new(), false)),
            ready: Condvar::new(),
        });
        let workers = (0..threads.max(1))
            .map(|_| {
                let queue = queue.clone();
                std::thread::spawn(move || {
                    loop {
                        let job = {
                            let mut guard = queue.jobs.lock().unwrap();
                            loop {
                                if let Some(job) = guard.0.pop() {
                                    break job;
                                }
                                if guard.1 {
                                    return;
                                }
                                guard = queue.ready.wait(guard).unwrap();
                            }
                        };
                        job();
                    }
                })
            })
            .collect();
        Ok(Self {
            inner: Arc::new(FileInner {
                directory,
                direct: direct && cfg!(target_os = "linux"),
                segment_bits,
                segments: Mutex::new(HashMap::new()),
            }),
            queue,
            workers,
        })
    }

    fn submit(&self, job: Job) {
        self.queue.jobs.lock().unwrap().0.push(job);
        self.queue.ready.notify_one();
    }
}

impl Device for FileDevice {
    unsafe fn write_async(&self, offset: u64, data: *const u8, len: usize, done: WriteCallback) {
        let inner = self.inner.clone();
        let data = SendPtr(data);
        self.submit(Box::new(move || {
            // SAFETY: the caller keeps the buffer valid until `done` runs.
            let bytes = unsafe { std::slice::from_raw_parts(data.get(), len) };
            done(inner.for_each_piece(offset, len, |file, at, from, piece| {
                file.write_all_at(&bytes[from..from + piece], at)
            }));
        }));
    }

    fn read_async(&self, offset: u64, len: usize, done: ReadCallback) {
        if !self.inner.direct {
            // Buffered reads are served from the page cache; like libaio on a
            // buffered file, complete them on the caller instead of handing
            // them to another thread. The caller still sees `Pending`.
            return done(self.inner.read(offset, len));
        }
        let inner = self.inner.clone();
        self.submit(Box::new(move || done(inner.read(offset, len))));
    }
}

impl Drop for FileDevice {
    fn drop(&mut self) {
        self.queue.jobs.lock().unwrap().1 = true;
        self.queue.ready.notify_all();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn round_trip(direct: bool) {
        let dir =
            std::env::temp_dir().join(format!("raster-device-{direct}-{}", std::process::id()));
        let device = FileDevice::open_with(&dir, 13, 2, direct).unwrap();
        // Sector-aligned source buffer and offset, as page flushes are.
        let mut data = AlignedBuffer::new(12_288);
        for (i, byte) in data.as_mut().iter_mut().enumerate() {
            *byte = i as u8;
        }
        let (tx, rx) = mpsc::channel();
        // SAFETY: `data` outlives the wait below.
        unsafe {
            device.write_async(
                4096,
                data.ptr,
                data.len(),
                Box::new(move |r| tx.send(r).unwrap()),
            )
        };
        rx.recv().unwrap().unwrap();
        let (tx, rx) = mpsc::channel();
        // Unaligned read spanning a segment boundary.
        device.read_async(8000, 300, Box::new(move |r| tx.send(r).unwrap()));
        assert_eq!(rx.recv().unwrap().unwrap(), &data.as_mut()[3904..4204]);
        drop(device);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn buffered_round_trip_across_segments() {
        round_trip(false);
    }

    #[test]
    fn direct_round_trip_across_segments() {
        round_trip(true);
    }
}
