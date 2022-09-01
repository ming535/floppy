use crate::common::error::Result;
use crate::pgwire::codec::FramedConn;
use std::future::Future;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, instrument};

struct Listener {
    listener: TcpListener,
}

impl Listener {
    async fn run(&mut self) -> Result<()> {
        info!("accepting inbound connections");

        loop {
            let (socket, addr) =
                self.listener.accept().await?;

            let mut handler = Handler {};
            tokio::spawn(async move {
                handler.run(socket).await;
            });
        }
    }
}

/// Per-connection handler. Read requests from `connection`
struct Handler {
    // conn: TcpStream,
}

impl Handler {
    async fn run(&mut self, conn: TcpStream) {
        info!("handle connection");
        FramedConn::new(1, conn);
    }
}

pub async fn run(
    listener: TcpListener,
    shutdown: impl Future,
) {
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
