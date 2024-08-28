use std::path::Path;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::{spawn_blocking, JoinHandle};

use crate::asyncdb::ReceiverExt;
use crate::asyncdb::CHANNEL_BUFFER_SIZE;
use crate::asyncdb::{Request, Response};
use crate::{Options, Result, Status, StatusCode, DB};

pub(crate) struct Message {
    pub(crate) req: Request,
    pub(crate) resp_channel: oneshot::Sender<Response>,
}

/// `AsyncDB` makes it easy to use LevelDB in a tokio runtime.
/// The methods follow very closely the main API (see `DB` type). Iteration is not yet implemented.
#[derive(Clone)]
pub struct AsyncDB {
    jh: Arc<JoinHandle<()>>,
    send: mpsc::Sender<Message>,
}

impl AsyncDB {
    /// Create a new or open an existing database.
    pub fn new<P: AsRef<Path>>(name: P, opts: Options) -> Result<AsyncDB> {
        let db = DB::open(name, opts)?;
        let (send, recv) = mpsc::channel(CHANNEL_BUFFER_SIZE);

        let jh = spawn_blocking(move || AsyncDB::run_server(db, recv));
        Ok(AsyncDB {
            jh: Arc::new(jh),
            send,
        })
    }
    pub(crate) async fn process_request(&self, req: Request) -> Result<Response> {
        let (tx, rx) = oneshot::channel();

        let m = Message {
            req,
            resp_channel: tx,
        };
        if let Err(e) = self.send.send(m).await {
            return Err(Status {
                code: StatusCode::AsyncError,
                err: e.to_string(),
            });
        }
        let resp = rx.await;

        match resp {
            Err(e) => Err(Status {
                code: StatusCode::AsyncError,
                err: e.to_string(),
            }),
            Ok(r) => Ok(r),
        }
    }
}

pub(crate) fn send_response_result(ch: oneshot::Sender<Response>, result: Result<()>) {
    if let Err(e) = result {
        ch.send(Response::Error(e)).ok();
    } else {
        ch.send(Response::OK).ok();
    }
}

pub(crate) fn send_response(ch: oneshot::Sender<Response>, res: Response) {
    ch.send(res).ok();
}

impl<T> ReceiverExt<T> for mpsc::Receiver<T> {
    fn blocking_recv(&mut self) -> Option<T> {
        self.blocking_recv()
    }

    fn close(&mut self) {
        mpsc::Receiver::close(self);
    }
}
