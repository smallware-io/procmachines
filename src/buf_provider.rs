use core::task::Poll;

use bytes::BytesMut;

/// A trait for providing `BytesMut` buffers to read data into.
/// Implemnetations of this trait determine the buffer size and recycling strategy.
/// `ReaderIoReader` uses this to get buffers for its internal `poll_read_buf` calls.
pub trait AsyncBufProvider {
    fn poll_get_buf(&mut self) -> Poll<BytesMut>;
}

/// A simple `AsyncBufProvider` that cycles through a fixed array of buffers.  Whenever a
/// new buffer is required, it attempts to recycle the least-recently returned buffer that
/// it has.  If its reference to that buffer is not unique, then it will drop it and
/// allocate a new one.
pub struct CyclicBufProvider<const COUNT: usize, FALLOC>
where
    FALLOC: Fn() -> BytesMut,
{
    bufs: [BytesMut; COUNT],
    sizes: [usize; COUNT],
    next_idx: usize,
    f_alloc: FALLOC,
}

impl<const COUNT: usize, FALLOC> CyclicBufProvider<COUNT, FALLOC>
where
    FALLOC: Fn() -> BytesMut,
{
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
