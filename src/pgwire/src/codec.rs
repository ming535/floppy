//! Encoding/decoding of messages in pgwire. See "[Frontend/Backend Protocol:
//! Message Formats][1]" in the PostgreSQL reference for the specification.
//!
//! [1]: https://www.postgresql.org/docs/11/protocol-message-formats.html

use crate::message::ErrorResponse;
use crate::message::{
    BackendMessage, FrontendMessage,
    FrontendStartupMessage, TransactionStatus,
    VERSION_CANCEL, VERSION_GSSENC, VERSION_SSL,
};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, BytesMut};
use common::error::{FloppyError, Result};
use futures::{sink, SinkExt, TryStreamExt};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use tokio::io::{
    self, AsyncRead, AsyncReadExt, AsyncWrite,
};
use tokio_util::codec::{Decoder, Encoder, Framed};

pub const REJECT_ENCRYPTION: u8 = b'N';

#[derive(Debug)]
enum CodecError {
    StringNoTerminator,
}

impl Error for CodecError {}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CodecError::StringNoTerminator => {
                "The string does not have a terminator"
            }
        })
    }
}

pub async fn decode_startup<A>(
    mut conn: A,
) -> Result<Option<FrontendStartupMessage>>
where
    A: AsyncRead + Unpin,
{
    let mut frame_len = [0; 4];
    let n = conn.read_exact(&mut frame_len).await?;
    match n {
        // Complete frame length. Continue.
        4 => (),
        // Partial frame length. Likely a client bug or network glitch, so
        // surface the unexpected EOF.
        _ => {
            return Err(FloppyError::from(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "early eof",
            )))
        }
    }

    let frame_len = parse_frame_len(&frame_len)?;
    let mut buf = BytesMut::new();
    buf.resize(frame_len, b'0');
    conn.read_exact(&mut buf).await?;

    let mut buf = Cursor::new(&buf);
    let version = buf.read_i32()?;
    let message = match version {
        VERSION_CANCEL => {
            FrontendStartupMessage::CancelRequest {
                conn_id: buf.read_u32()?,
                secret_key: buf.read_u32()?,
            }
        }
        VERSION_SSL => FrontendStartupMessage::SslRequest,
        VERSION_GSSENC => {
            FrontendStartupMessage::GssEncRequest
        }
        _ => {
            let mut params = HashMap::new();
            while buf.peek_byte()? != 0 {
                let name = buf.read_cstr()?.to_owned();
                let value = buf.read_cstr()?.to_owned();
                params.insert(name, value);
            }
            FrontendStartupMessage::Startup {
                version,
                params,
            }
        }
    };

    Ok(Some(message))
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

fn parse_frame_len(
    src: &[u8],
) -> std::result::Result<usize, io::Error> {
    let n = NetworkEndian::read_u32(src) as usize;
    if n < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid frame length",
        ));
    }
    Ok(n - 4)
}

struct Codec {
    decode_state: DecodeState,
}

#[derive(Debug)]
enum DecodeState {
    Head,
    Data(u8, usize),
}

impl Codec {
    pub fn new() -> Codec {
        Codec {
            decode_state: DecodeState::Head,
        }
    }
}

impl Default for Codec {
    fn default() -> Self {
        Codec::new()
    }
}

impl Encoder<BackendMessage> for Codec {
    type Error = io::Error;

    fn encode(
        &mut self,
        msg: BackendMessage,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        let byte = match &msg {
            BackendMessage::AuthenticationOk => b'R',
            BackendMessage::EmptyQueryResponse => b'I',
            BackendMessage::ReadyForQuery(_) => b'Z',
            BackendMessage::ErrorResponse(r) => {
                if r.severity.is_error() {
                    b'E'
                } else {
                    b'N'
                }
            }
        };
        dst.put_u8(byte);

        // Write message length placeholder. The true length is filled in later.
        let base = dst.len();
        dst.put_u32(0);

        // Write message contents
        match msg {
            BackendMessage::AuthenticationOk => {
                dst.put_u32(0);
            }
            BackendMessage::EmptyQueryResponse => (),
            BackendMessage::ReadyForQuery(status) => {
                dst.put_u8(match status {
                    TransactionStatus::Idle => b'I',
                    TransactionStatus::InTransaction => {
                        b'T'
                    }
                    TransactionStatus::Failed => b'E',
                });
            }
            BackendMessage::ErrorResponse(
                ErrorResponse {
                    severity,
                    code,
                    message,
                    detail,
                    hint,
                    position,
                },
            ) => {
                dst.put_u8(b'S');
                dst.put_string(severity.as_str());
                dst.put_u8(b'C');
                dst.put_string(code.code());
                dst.put_u8(b'M');
                dst.put_string(&message);
                if let Some(detail) = &detail {
                    dst.put_u8(b'D');
                    dst.put_string(detail);
                }
                if let Some(hint) = &hint {
                    dst.put_u8(b'H');
                    dst.put_string(hint);
                }
                if let Some(position) = &position {
                    dst.put_u8(b'P');
                    dst.put_string(&position.to_string());
                }
                dst.put_u8(b'\0');
            }
        }

        let len = dst.len() - base;

        // Overwrite length placeholder with true length.
        let len = i32::try_from(len).map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "length of encoded message does not fit into an i32",)
        })?;
        dst[base..base + 4]
            .copy_from_slice(&len.to_be_bytes());
        Ok(())
    }
}

trait Pgbuf: BufMut {
    fn put_string(&mut self, s: &str);
    fn put_length_i16(
        &mut self,
        len: usize,
    ) -> std::result::Result<(), io::Error>;
    fn put_format_i8(&mut self, format: pgrepr::Format);
    fn put_format_i16(&mut self, format: pgrepr::Format);
}

impl<B: BufMut> Pgbuf for B {
    fn put_string(&mut self, s: &str) {
        self.put(s.as_bytes());
        self.put_u8(b'\0');
    }

    fn put_length_i16(
        &mut self,
        len: usize,
    ) -> std::result::Result<(), io::Error> {
        let len = i16::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "length does not fit in an i16",
            )
        })?;
        self.put_i16(len);
        Ok(())
    }

    fn put_format_i8(&mut self, format: pgrepr::Format) {
        self.put_i8(match format {
            pgrepr::Format::Text => 0,
            pgrepr::Format::Binary => 1,
        })
    }

    fn put_format_i16(&mut self, format: pgrepr::Format) {
        self.put_i8(0);
        self.put_format_i8(format);
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
        loop {
            match self.decode_state {
                DecodeState::Head => {
                    if src.len() < 5 {
                        return Ok(None);
                    }
                    let msg_type = src[0];
                    let frame_len =
                        parse_frame_len(&src[1..])?;
                    src.advance(5);
                    src.reserve(frame_len);
                    self.decode_state = DecodeState::Data(
                        msg_type, frame_len,
                    );
                }

                DecodeState::Data(msg_type, frame_len) => {
                    if src.len() < frame_len {
                        return Ok(None);
                    }
                    let buf =
                        src.split_to(frame_len).freeze();
                    let buf = Cursor::new(&buf);

                    let msg = match msg_type {
                        // Simple query flow.
                        b'Q' => decode_query(buf)?,

                        // Extended query flow.

                        // Invalid
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                "decode frame unknown message type {}",
                                char::from(msg_type)
                            ),
                            ))
                        }
                    };
                    src.reserve(5);
                    self.decode_state = DecodeState::Head;
                    return Ok(Some(msg));
                }
            }
        }
    }
}

/// Decodes data within pgwire messages.
///
/// The API provided is very similar to [`bytes::Buf`], but operations return
/// errors rather than panicking. This is important for safety, as we don't want
/// to crash if the user sends us malformed pgwire messages.
///
/// There are also some special-purpose methods, like [`Cursor::read_cstr`],
/// that are specific to pgwire messages.
#[derive(Debug)]
struct Cursor<'a> {
    buf: &'a [u8],
}

impl<'a> Cursor<'a> {
    /// Constructs a new `Cursor` from a byte slice. The cursor will begin
    /// decoding from the beginning of the slice.
    fn new(buf: &'a [u8]) -> Cursor {
        Cursor { buf }
    }

    /// Returns the next byte without advancing the cursor.
    fn peek_byte(&self) -> Result<u8> {
        self.buf.get(0).copied().ok_or_else(|| {
            FloppyError::from(input_err("No byte to read"))
        })
    }

    /// Returns the next byte, advancing the cursor by one byte.
    fn read_byte(&mut self) -> Result<u8> {
        let byte = self.peek_byte()?;
        self.advance(1);
        Ok(byte)
    }

    /// Returns the next null-terminated string. The null character is not
    /// included the returned string. The cursor is advanced past the null-
    /// terminated string.
    ///
    /// If there is no null byte remaining in the string, returns
    /// `CodecError::StringNoTerminator`. If the string is not valid UTF-8,
    /// returns an `io::Error` with an error kind of
    /// `io::ErrorKind::InvalidInput`.
    ///
    /// NOTE(benesch): it is possible that returning a string here is wrong, and
    /// we should be returning bytes, so that we can support messages that are
    /// not UTF-8 encoded. At the moment, we've not discovered a need for this,
    /// though, and using proper strings is convenient.
    fn read_cstr(
        &mut self,
    ) -> std::result::Result<&'a str, io::Error> {
        if let Some(pos) =
            self.buf.iter().position(|b| *b == 0)
        {
            let val = std::str::from_utf8(&self.buf[..pos])
                .map_err(input_err)?;
            self.advance(pos + 1);
            Ok(val)
        } else {
            Err(input_err(CodecError::StringNoTerminator))
        }
    }

    /// Reads the next 16-bit signed integer, advancing the cursor by two
    /// bytes.
    fn read_i16(&mut self) -> Result<i16> {
        if self.buf.len() < 2 {
            return Err(FloppyError::from(input_err(
                "not enough buffer for an Int16",
            )));
        }
        let val = NetworkEndian::read_i16(self.buf);
        self.advance(2);
        Ok(val)
    }

    /// Reads the next 32-bit signed integer, advancing the cursor by four
    /// bytes.
    fn read_i32(&mut self) -> Result<i32> {
        if self.buf.len() < 4 {
            return Err(FloppyError::from(input_err(
                "not enough buffer for an Int32",
            )));
        }
        let val = NetworkEndian::read_i32(self.buf);
        self.advance(4);
        Ok(val)
    }

    /// Reads the next 32-bit unsigned integer, advancing the cursor by four
    /// bytes.
    fn read_u32(&mut self) -> Result<u32> {
        if self.buf.len() < 4 {
            return Err(FloppyError::from(input_err(
                "not enough buffer for an Int32",
            )));
        }
        let val = NetworkEndian::read_u32(self.buf);
        self.advance(4);
        Ok(val)
    }

    /// Reads the next 16-bit format code, advancing the cursor by two bytes.
    fn read_format(&mut self) -> Result<pgrepr::Format> {
        match self.read_i16()? {
            0 => Ok(pgrepr::Format::Text),
            1 => Ok(pgrepr::Format::Binary),
            n => Err(FloppyError::from(input_err(
                format!("unknown format code: {}", n),
            ))),
        }
    }

    /// Advances the cursor by `n` bytes.
    fn advance(&mut self, n: usize) {
        self.buf = &self.buf[n..]
    }
}

fn input_err(
    source: impl Into<Box<dyn Error + Send + Sync>>,
) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        source.into(),
    )
}

fn decode_query(
    mut buf: Cursor,
) -> std::result::Result<FrontendMessage, io::Error> {
    Ok(FrontendMessage::Query {
        sql: buf.read_cstr()?.to_string(),
    })
}
