use inotify::{Inotify, WatchMask};
use inotify_sys as ffi;
use std::io;
use std::mem;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::Path;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_uring::buf::IoBuf;
use tokio_uring::fs::File;

const EVENT_SIZE: usize = mem::size_of::<ffi::inotify_event>();

#[inline]
fn clone_error(error: &std::io::Error) -> io::Error {
    io::Error::new(error.kind(), error.to_string())
}

async fn wake_up_stream_task(
    watch: inotify::Inotify,
    tx: Sender<io::Result<()>>,
) -> io::Result<()> {
    // Safety: this is safe because watch_file will be dropped before
    // watch.
    let watch_file = unsafe { File::from_raw_fd(watch.as_raw_fd()) };

    let mut buf = vec![0; 4096];

    loop {
        let watch_file = &watch_file;
        let (res, ebuf) = watch_file.read_at(buf, 0).await;
        let n = match res {
            Err(e) => {
                let dup_err = clone_error(&e);
                let _ = tx.send(Err(e)).await;
                return Err(dup_err);
            }
            Ok(n) => n,
        };
        assert!(n >= EVENT_SIZE);

        let mut offset = 0;
        while offset < n {
            let (_, _event) = event_from_buffer(&ebuf[offset..offset + EVENT_SIZE]);

            match tx.try_send(Ok(())) {
                Ok(_) => {}
                Err(err) => match err {
                    TrySendError::Full(_) => {} // nevermind, we only want to keep one value in the buffer and discard the rest
                    TrySendError::Closed(_) => return Ok(()), // we're done.
                },
            }
            offset += EVENT_SIZE;
        }
        buf = ebuf
    }
}

fn wake_up_stream(path: impl AsRef<Path>) -> io::Result<Receiver<io::Result<()>>> {
    let mut watch = Inotify::init()?;
    // Watch for modify events.
    watch.add_watch(path, WatchMask::MODIFY)?;
    let (tx, rx) = channel(1);

    tokio_uring::spawn(wake_up_stream_task(watch, tx));
    Ok(rx)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio_uring::start(async {
        let args: Vec<String> = std::env::args().collect();
        let path = &args[1];

        let mut wakeups = wake_up_stream(path.as_str())?;

        let file = File::open(path.as_str()).await?;
        let out = unsafe { File::from_raw_fd(1) };

        let mut offset = 0;
        let mut buf = vec![0; 64 << 10];

        loop {
            let (res, rbuf) = file.read_at(buf, offset).await;
            let n = res?;
            if n == 0 {
                // eprintln!("wait for event");
                // since channel is buffered of 1 item we may do spurious reads.
                buf = rbuf;
                // if
                wakeups.recv().await.expect("no close")?;
            } else {
                // eprintln!("read {}", n);
                let mut wbuf = rbuf;
                let mut woffset = 0;
                while woffset < n {
                    let (res, abuf) = out.write_at(wbuf.slice(woffset..n), 0).await;
                    woffset += res?;
                    wbuf = abuf.into_inner();
                }
                buf = wbuf;
            }
            offset += n as u64;
        }
    })
}

/// Create an `inotify_event` from a buffer
///
/// Assumes that a full `inotify_event` plus its name is located at the
/// beginning of `buffer`.
///
/// Returns the number of bytes used from the buffer, and the event.
///
/// # Panics
///
/// Panics if the buffer does not contain a full event, including its name.
fn event_from_buffer(buffer: &[u8]) -> (usize, &ffi::inotify_event) {
    let event_size = mem::size_of::<ffi::inotify_event>();
    let event_align = mem::align_of::<ffi::inotify_event>();

    // Make sure that the buffer can satisfy the alignment requirements for `inotify_event`
    assert!(buffer.len() >= event_align);

    // Discard the unaligned portion, if any, of the supplied buffer
    let buffer = align_buffer(buffer);

    // Make sure that the aligned buffer is big enough to contain an event, without
    // the name. Otherwise we can't safely convert it to an `inotify_event`.
    assert!(buffer.len() >= event_size);

    let event = buffer.as_ptr() as *const ffi::inotify_event;

    // We have a pointer to an `inotify_event`, pointing to the beginning of
    // `buffer`. Since we know, as per the assertion above, that there are
    // enough bytes in the buffer for at least one event, we can safely
    // convert that pointer into a reference.
    let event = unsafe { &*event };

    // The name's length is given by `event.len`. There should always be
    // enough bytes left in the buffer to fit the name. Let's make sure that
    // is the case.
    let bytes_left_in_buffer = buffer.len() - event_size;
    assert!(bytes_left_in_buffer >= event.len as usize);

    // Directly after the event struct should be a name, if there's one
    // associated with the event. Let's make a new slice that starts with
    // that name. If there's no name, this slice might have a length of `0`.
    let bytes_consumed = event_size + event.len as usize;

    (bytes_consumed, event)
}

fn align_buffer(buffer: &[u8]) -> &[u8] {
    if buffer.len() >= mem::align_of::<ffi::inotify_event>() {
        let ptr = buffer.as_ptr();
        let offset = ptr.align_offset(mem::align_of::<ffi::inotify_event>());
        &buffer[offset..]
    } else {
        &buffer[0..0]
    }
}
