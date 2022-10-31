use crate::codec::{FramedConn, REJECT_ENCRYPTION};
use crate::message::FrontendStartupMessage;
use crate::{codec, protocol};
use common::error::Result;
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, instrument, warn};

struct Listener {
    listener: TcpListener,
}

impl Listener {
    async fn run(&mut self) -> Result<()> {
        info!("accepting inbound connections");
        let conn_id = Arc::new(AtomicU32::new(1));

        loop {
            let (conn, addr) = self.listener.accept().await?;

            let mut handler = Handler {};
            let conn_id = conn_id.clone();
            tokio::spawn(async move {
                let conn_id = conn_id.fetch_add(1, Ordering::SeqCst);
                if let Err(e) = handler.run(conn_id, conn).await {
                    error!("error handling connection {}: {}", conn_id, e)
                }
            });
        }
    }
}

/// Per-connection handler. Read requests from `connection`
struct Handler {
    // conn: TcpStream,
}

impl Handler {
    async fn run(&mut self, conn_id: u32, mut conn: TcpStream) -> Result<()> {
        info!("handle connection");
        loop {
            let message = codec::decode_startup(&mut conn).await?;
            conn = match message {
                // Clients sometimes hang up during the startup sequence, e.g.
                // because they receive an unacceptable response to an
                // `SslRequest`. This is considered a graceful termination.
                None => {
                    return Ok(());
                }
                Some(FrontendStartupMessage::Startup { version, params }) => {
                    info!("startup conn {}", conn_id);
                    let mut conn = FramedConn::new(conn_id, conn);
                    protocol::run(conn_id, &mut conn).await?;
                    return Ok(());
                }
                Some(FrontendStartupMessage::SslRequest) => {
                    conn.write_all(&[REJECT_ENCRYPTION]).await?;
                    conn
                }
                _ => {
                    warn!("not supported now: {:?}", message);
                    return Ok(());
                }
            }
        }

        Ok(())
    }
}

pub async fn run(listener: TcpListener, shutdown: impl Future) {
    let mut server = Listener { listener };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(case = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }

    info!("exit");
}
