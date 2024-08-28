use std::path::Path;
use std::sync::Arc;

use async_std::channel;
use async_std::task::{spawn_blocking, JoinHandle};

use crate::asyncdb::{ReceiverExt, Request, Response, CHANNEL_BUFFER_SIZE};
use crate::{Options, Result, Status, StatusCode, DB};

pub(crate) struct Message {
    pub(crate) req: Request,
    pub(crate) resp_channel: channel::Sender<Response>,
}
/// `AsyncDB` makes it easy to use LevelDB in a async-std runtime.
/// The methods follow very closely the main API (see `DB` type). Iteration is not yet implemented.
#[derive(Clone)]
pub struct AsyncDB {
    jh: Arc<JoinHandle<()>>,
    send: channel::Sender<Message>,
}

impl AsyncDB {
    /// Create a new or open an existing database.
    pub fn new<P: AsRef<Path>>(name: P, opts: Options) -> Result<AsyncDB> {
        let db = DB::open(name, opts)?;

        let (send, recv) = channel::bounded(CHANNEL_BUFFER_SIZE);
        let jh = spawn_blocking(move || AsyncDB::run_server(db, recv));
        Ok(AsyncDB {
            jh: Arc::new(jh),
            send,
        })
    }

    pub(crate) async fn process_request(&self, req: Request) -> Result<Response> {
        let (tx, rx) = channel::bounded(1);

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
        let resp = rx.recv().await;
        match resp {
            Err(e) => Err(Status {
                code: StatusCode::AsyncError,
                err: e.to_string(),
            }),
            Ok(r) => Ok(r),
        }
    }
}

pub(crate) fn send_response_result(ch: channel::Sender<Response>, result: Result<()>) {
    if let Err(e) = result {
        ch.try_send(Response::Error(e)).ok();
    } else {
        ch.try_send(Response::OK).ok();
    }
}

pub(crate) fn send_response(ch: channel::Sender<Response>, res: Response) {
    ch.send_blocking(res).ok();
}

impl<T> ReceiverExt<T> for channel::Receiver<T> {
    fn blocking_recv(&mut self) -> Option<T> {
        self.recv_blocking().ok()
    }

    fn close(&mut self) {
        channel::Receiver::close(self);
    }
}
