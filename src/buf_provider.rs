//! Buffer providers for byte-stream readers.
//!
//! An [`AsyncBufProvider`] supplies the [`BytesMut`] buffers that a byte reader
//! reads into, decoupling the read loop from the buffer-sizing and recycling
//! policy.  [`CyclicBufProvider`] is a ready-made implementation that recycles
//! a fixed pool of buffers.

use core::task::Poll;

use bytes::BytesMut;

/// A trait for providing [`BytesMut`] buffers to read data into.
///
/// Implementations of this trait determine the buffer size and recycling
/// strategy.  [`ReaderIoReader`](crate::tokio_reader_writer::ReaderIoReader)
/// uses this to obtain buffers for its internal reads.
pub trait AsyncBufProvider {
    /// Attempts to provide a buffer to read into.
    ///
    /// Returns [`Poll::Ready`] with a buffer ready to be filled, or
    /// [`Poll::Pending`] if no buffer is currently available (in which case the
    /// caller's waker must have been registered for later notification).
    fn poll_get_buf(&mut self) -> Poll<BytesMut>;
}

/// A simple `AsyncBufProvider` that cycles through a fixed array of buffers.  Whenever a
/// new buffer is required, it attempts to recycle the least-recently returned buffer that
/// it has.  If its reference to that buffer is not unique, then it will drop it and
/// allocate a new one.
#[cfg(feature = "std")]
pub struct CyclicBufProvider<const COUNT: usize, FALLOC>
where
    FALLOC: Fn() -> BytesMut,
{
    bufs: [BytesMut; COUNT],
    sizes: [usize; COUNT],
    next_idx: usize,
    f_alloc: FALLOC,
}

#[cfg(feature = "std")]
impl<const COUNT: usize, FALLOC> CyclicBufProvider<COUNT, FALLOC>
where
    FALLOC: Fn() -> BytesMut,
{
    /// Creates a new provider that allocates buffers with `f_alloc` and
    /// recycles up to `COUNT` of them.
    ///
    /// # Panics
    ///
    /// Panics if `COUNT` is zero.
    pub fn new(f_alloc: FALLOC) -> Self {
        if COUNT < 1 {
            panic!("CyclicBufProvider requires COUNT >= 1");
        }
        Self {
            bufs: core::array::from_fn(|_| BytesMut::new()),
            sizes: [0; COUNT],
            next_idx: 0,
            f_alloc,
        }
    }
}

#[cfg(feature = "std")]
impl<const COUNT: usize, FALLOC> AsyncBufProvider for CyclicBufProvider<COUNT, FALLOC>
where
    FALLOC: Fn() -> BytesMut,
{
    fn poll_get_buf(&mut self) -> Poll<BytesMut> {
        let target = &mut self.bufs[self.next_idx];
        let szref = &mut self.sizes[self.next_idx];
        self.next_idx = (self.next_idx + 1) % COUNT;

        if *szref < 1 || !target.try_reclaim(*szref) {
            *target = (self.f_alloc)();
            *szref = target.capacity();
        }
        Poll::Ready(target.split_off(0))
    }
}
