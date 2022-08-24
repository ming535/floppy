use crate::common::error::Result;
use std::future::Future;
use tokio::net::TcpListener;
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
            info!("new connection from {}", addr);
        }
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
