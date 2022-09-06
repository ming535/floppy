use crate::common::error::Result;
use crate::pgwire::codec::FramedConn;
use crate::pgwire::message::{
    BackendMessage, TransactionStatus,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tracing::{debug, error, info, instrument, warn};

pub async fn run<A>(
    conn_id: u32,
    conn: &mut FramedConn<A>,
) -> Result<()>
where
    A: AsyncRead + AsyncWrite + Send + Sync + Unpin,
{
    // todo! create a new session here
    let mut buf = vec![BackendMessage::AuthenticationOk];
    buf.push(BackendMessage::ReadyForQuery(
        TransactionStatus::Idle,
    ));
    conn.send_all(buf).await?;
    conn.flush().await?;
    info!("ready for query");
    Ok(())
}
