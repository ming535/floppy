//! Encoding/decoding of messages in pgwire. See "[Frontend/Backend Protocol:
//! Message Formats][1]" in the PostgreSQL reference for the specification.
//!
//! [1]: https://www.postgresql.org/docs/11/protocol-message-formats.html

use crate::common::error::Result;
use crate::pgwire::message::{
    BackendMessage, FrontendMessage, FrontendStartupMessage,
};
use bytes::{Buf, BufMut, BytesMut};
use futures::{sink, SinkExt, TryStreamExt};
use tokio::io::{
    self, AsyncRead, AsyncReadExt, AsyncWrite,
};
use tokio_util::codec::{Decoder, Encoder, Framed};

pub async fn decode_startup(
) -> Result<Option<FrontendStartupMessage>> {
    Ok(None)
}

/// A connection that manages the encoding and decoding of pgwire frames.
pub struct FramedConn<A> {
    conn_id: u32,
    inner: sink::Buffer<Framed<A, Codec>, BackendMessage>,
}

impl<A> FramedConn<A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(conn_id: u32, inner: A) -> FramedConn<A> {
        FramedConn {
            conn_id,
            inner: Framed::new(inner, Codec::new())
                .buffer(32),
        }
    }

    /// Reads and decodes one frontend message from the client.
    ///
    /// Blocks until the client sends a complete message. If the client
    /// terminates the stream, returns `None`. Returns an error if the client
    /// sends a malformed message or if the connection underlying is broken.
    pub async fn recv(
        &mut self,
    ) -> Result<Option<FrontendMessage>> {
        let message = self.inner.try_next().await?;
        Ok(message)
    }

    /// Encodes and sends one backend message to the client.
    ///
    /// Note that the connection is not flushed after calling this method. You
    /// must call [`FramedConn::flush`] explicitly. Returns an error if the
    /// underlying connection is broken.
    ///
    /// Please use `StateMachine::send` instead if calling from `StateMachine`,
    /// as it applies session-based filters before calling this method.
    pub async fn send<M>(
        &mut self,
        message: M,
    ) -> Result<()>
    where
        M: Into<BackendMessage>,
    {
        let message = message.into();
        Ok(self.inner.send(message).await?)
    }

    /// Encodes and sends the backend messages in the `messages` iterator to the
    /// client.
    ///
    /// As with [`FramedConn::send`], the connection is not flushed after
    /// calling this method. You must call [`FramedConn::flush`] explicitly.
    /// Returns an error if the underlying connection is broken.
    pub async fn send_all(
        &mut self,
        messages: impl IntoIterator<Item = BackendMessage>,
    ) -> Result<()> {
        // we intentionally don't use `self.conn.send_all` here to avoid
        // flushing the sink unnecessarily.
        for m in messages {
            self.send(m).await?;
        }
        Ok(())
    }

    /// Flushes all outstanding messages.
    pub async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await?;
        Ok(())
    }
}

struct Codec {}

impl Codec {
    pub fn new() -> Codec {
        Codec {}
    }
}

impl Encoder<BackendMessage> for Codec {
    type Error = io::Error;

    fn encode(
        &mut self,
        item: BackendMessage,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        todo!()
    }
}

impl Decoder for Codec {
    type Item = FrontendMessage;
    type Error = io::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error>
    {
        todo!()
    }
}
