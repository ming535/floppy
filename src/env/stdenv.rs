use std::{
    fs::{File, OpenOptions},
    future::Future,
    io::Result,
    os::fd::AsRawFd,
    pin::Pin,
    task::{Context, Poll},
    thread,
};

use futures::executor::block_on;

use super::*;

/// An implementation of [`Env`] based on [`std`].
#[derive(Clone, Debug)]
pub struct StdEnv;

#[async_trait]
impl Env for StdEnv {
    type PositionalReaderWriter = PositionalReaderWriter;
    type JoinHandle<T: Send> = JoinHandle<T>;
    type Directory = Directory;

    async fn open_file<P>(
        &self,
        path: P,
    ) -> Result<Self::PositionalReaderWriter>
    where
        P: AsRef<Path> + Send,
    {
        Ok(PositionalReaderWriter(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?,
        ))
    }

    fn spawn_background<F>(&self, f: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let handle = thread::spawn(move || block_on(f));
        JoinHandle {
            handle: Some(handle),
        }
    }

    /// An async version of [`std::fs::rename`].
    async fn rename<P: AsRef<Path> + Send, Q: AsRef<Path> + Send>(
        &self,
        from: P,
        to: Q,
    ) -> Result<()> {
        std::fs::rename(from, to)
    }

    /// An async version of [`std::fs::remove_file`].
    async fn remove_file<P: AsRef<Path> + Send>(&self, path: P) -> Result<()> {
        std::fs::remove_file(path)
    }

    /// An async version of [`std::fs::create_dir`].
    async fn create_dir_all<P: AsRef<Path> + Send>(
        &self,
        path: P,
    ) -> Result<()> {
        std::fs::create_dir_all(path)
    }

    /// An async version of [`std::fs::remove_dir`].
    async fn remove_dir_all<P: AsRef<Path> + Send>(
        &self,
        path: P,
    ) -> Result<()> {
        std::fs::remove_dir_all(path)
    }

    /// Returns an iterator over the entries within a directory.
    /// See alos [`std::fs::read_dir`].
    fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<std::fs::ReadDir> {
        std::fs::read_dir(path)
    }

    async fn metadata<P: AsRef<Path> + Send>(
        &self,
        path: P,
    ) -> Result<Metadata> {
        let raw_metadata = std::fs::metadata(path)?;
        Ok(Metadata {
            len: raw_metadata.len(),
            is_dir: raw_metadata.is_dir(),
        })
    }

    async fn open_dir<P: AsRef<Path> + Send>(
        &self,
        path: P,
    ) -> Result<Self::Directory> {
        let file = File::open(path)?;
        if !file.metadata()?.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotADirectory,
                "not a dir",
            ));
        }
        Ok(Directory(file))
    }
}

pub struct PositionalReaderWriter(File);

#[async_trait]
impl super::PositionalWriter for PositionalReaderWriter {
    type WriteAt<'a> = impl Future<Output = Result<usize>> + 'a;

    fn write_at<'a>(&'a self, buf: &'a [u8], offset: u64) -> Self::WriteAt<'a> {
        use std::os::unix::fs::FileExt;
        async move { self.0.write_at(buf, offset) }
    }

    async fn sync_data(&self) -> Result<()> {
        async move { self.0.sync_data() }.await
    }

    async fn sync_all(&self) -> Result<()> {
        async move { self.0.sync_all() }.await
    }
}

#[async_trait]
impl super::PositionalReader for PositionalReaderWriter {
    type ReadAt<'a> = impl Future<Output = Result<usize>> + 'a;

    #[cfg(unix)]
    fn read_at<'a>(
        &'a self,
        buf: &'a mut [u8],
        offset: u64,
    ) -> Self::ReadAt<'a> {
        use std::os::unix::fs::FileExt;
        async move { self.0.read_at(buf, offset) }
    }

    async fn file_size(&self) -> usize {
        todo!()
    }
}

impl super::DioEnabler for PositionalReaderWriter {
    fn direct_io_ify(&self) -> Result<()> {
        super::direct_io_ify(self.0.as_raw_fd())
    }
}

pub struct JoinHandle<T> {
    handle: Option<thread::JoinHandle<T>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        let handle = self.handle.take().unwrap();
        match handle.join() {
            Ok(v) => Poll::Ready(v),
            Err(e) => std::panic::resume_unwind(e),
        }
    }
}

pub struct Directory(File);

#[async_trait]
impl super::Directory for Directory {
    async fn sync_all(&self) -> Result<()> {
        self.0.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_write() -> Result<()> {
        let env = StdEnv;
        // 100 KB
        let offset = 100 * 1024;
        let path = "tmp_test_file";

        let file = env.open_file(path).await.unwrap();
        file.write_at(b"hello", offset).await.unwrap();
        file.sync_all().await.unwrap();

        let file = env.open_file(path).await.unwrap();
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
