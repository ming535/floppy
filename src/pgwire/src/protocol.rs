use crate::codec::FramedConn;
use crate::message::{
    BackendMessage, FrontendMessage, TransactionStatus,
};
use common::error::Result;
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

    let machine = StateMachine { conn };
    machine.run().await?;
    Ok(())
}

enum State {
    Ready,
    Drain,
    Done,
}

struct StateMachine<'a, A> {
    conn: &'a mut FramedConn<A>,
}

impl<'a, A> StateMachine<'a, A>
where
    A: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'a,
{
    async fn run(mut self) -> Result<()> {
        let mut state = State::Ready;
        loop {
            state = match state {
                State::Ready => {
                    self.advance_ready().await?
                }
                State::Drain => {
                    self.advance_drain().await?
                }
                State::Done => return Ok(()),
            }
        }
    }

    async fn advance_ready(&mut self) -> Result<State> {
        let message = self.conn.recv().await?;
        match message {
            Some(FrontendMessage::Query { sql }) => {
                info!("query: {}", sql);
            }
            _ => {
                warn!("unimplemented: {:?}", message)
            }
        }
        Ok(State::Ready)
    }

    async fn advance_drain(&mut self) -> Result<State> {
        todo!()
    }
}
