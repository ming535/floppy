use super::*;
use std::{
    fs::ReadDir,
    pin::Pin,
    slice,
    task::{Context, Poll},
    thread,
};
use tokio::sync::Mutex;

/// An implementation of [`Env`] based on simulation.
#[derive(Clone, Debug)]
pub struct SimEnv;

#[async_trait]
impl Env for SimEnv {
    type PositionalReaderWriter = SimMem;
    type JoinHandle<T: Send> = SimJoinHandle<T>;
    type Directory = SimDir;

    async fn open_file<P>(&self, path: P) -> Result<Self::PositionalReaderWriter>
    where
        P: AsRef<Path> + Send,
    {
        assert_eq!(path.as_ref(), Path::new("sim"));
        Ok(SimMem(Mutex::new(vec![])))
    }

    fn spawn_background<F>(&self, f: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        todo!()
    }

    async fn rename<P: AsRef<Path> + Send, Q: AsRef<Path> + Send>(
        &self,
        from: P,
        to: Q,
    ) -> Result<()> {
        todo!()
    }

    async fn remove_file<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
        Ok(())
    }

    async fn create_dir_all<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
        todo!()
    }

    async fn remove_dir_all<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
        todo!()
    }

    fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<ReadDir> {
        todo!()
    }

    async fn metadata<P: AsRef<Path> + Send>(&self, path: P) -> Result<Metadata> {
        todo!()
    }

    async fn open_dir<P: AsRef<Path> + Send>(&self, path: P) -> Result<Self::Directory> {
        todo!()
    }
}

pub struct SimMem(Mutex<Vec<u8>>);

#[async_trait]
impl super::PositionalWriter for SimMem {
    type WriteAt<'a> = impl Future<Output = Result<usize>> + 'a;

    fn write_at<'a>(&'a self, buf: &'a [u8], offset: u64) -> Self::WriteAt<'a> {
        async move {
            if buf.is_empty() {
                return Ok(0);
            }

            let mut data = self.0.lock().await;
            if offset + buf.len() as u64 > data.len() as u64 {
                data.resize(offset as usize + buf.len(), 0);
            }
            let dst = &mut (data[offset as usize..offset as usize + buf.len()]);
            dst.copy_from_slice(buf);
            Ok(buf.len())
        }
    }

    async fn sync_data(&self) -> Result<()> {
        Ok(())
    }

    async fn sync_all(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl PositionalReader for SimMem {
    type ReadAt<'a> = impl Future<Output = Result<usize>> + 'a;

    fn read_at<'a>(&'a self, buf: &'a mut [u8], offset: u64) -> Self::ReadAt<'a> {
        async move {
            let data = &self.0.lock().await;
            if offset > data.len() as u64 {
                return Ok(0);
            }

            let unread = &data[offset as usize..];
            let copy_size = std::cmp::min(unread.len(), buf.len());
            buf[..copy_size].copy_from_slice(&unread[..copy_size]);
            Ok(copy_size)
        }
    }

    async fn file_size(&self) -> usize {
        self.0.lock().await.len()
    }
}

impl DioEnabler for SimMem {
    fn direct_io_ify(&self) -> Result<()> {
        Ok(())
    }
}

pub struct SimJoinHandle<T> {
    handle: Option<thread::JoinHandle<T>>,
}

impl<T> Future for SimJoinHandle<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let handle = self.handle.take().unwrap();
        match handle.join() {
            Ok(v) => Poll::Ready(v),
            Err(e) => std::panic::resume_unwind(e),
        }
    }
}

pub struct SimDir;

#[async_trait]
impl super::Directory for SimDir {
    async fn sync_all(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_write() -> Result<()> {
        let env = SimEnv;
        // 100 KB
        let offset = 100 * 1024;
        let path = "sim";

        let mut file = env.open_file(path).await.unwrap();
        file.write_at(b"hello", offset).await.unwrap();
        file.sync_all().await.unwrap();

        let mut buf = [0u8; 5];
        file.read_at(&mut buf, offset).await.unwrap();
        assert_eq!(&buf, b"hello");

        // pos 0 content should be zero.
        file.read_exact_at(&mut buf, 0).await.unwrap();
        assert_eq!(&buf, &[0u8; 5]);

        // pos 200 content should be zero.
        let mut buf = [0u8; 100];
        file.read_exact_at(&mut buf, 200).await.unwrap();
        assert_eq!(&buf, &[0u8; 100]);

        env.remove_file(path).await.unwrap();
        Ok(())
    }
}
