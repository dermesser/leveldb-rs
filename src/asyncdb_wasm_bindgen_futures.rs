use std::collections::HashMap;
use std::path::Path;

use async_std::channel::{self, TryRecvError};
use wasm_bindgen_futures::spawn_local;

use crate::asyncdb::{ReceiverExt, Request, Response, CHANNEL_BUFFER_SIZE};
use crate::snapshot::Snapshot;
use crate::{Options, Result, Status, StatusCode, DB};

pub(crate) struct Message {
    pub(crate) req: Request,
    pub(crate) resp_channel: channel::Sender<Response>,
}

/// `AsyncDB` makes it easy to use LevelDB in a async-std runtime.
/// The methods follow very closely the main API (see `DB` type). Iteration is not yet implemented.
#[derive(Clone)]
pub struct AsyncDB {
    shutdown: channel::Sender<()>,
    send: channel::Sender<Message>,
}

impl AsyncDB {
    /// Create a new or open an existing database.
    pub fn new<P: AsRef<Path>>(name: P, opts: Options) -> Result<AsyncDB> {
        let db = DB::open(name, opts)?;

        let (send, recv) = channel::bounded(CHANNEL_BUFFER_SIZE);
        let (shutdown, shutdown_recv) = channel::bounded(1);

        spawn_local(async move {
            AsyncDB::run_server_async(db, recv, shutdown_recv, HashMap::new(), 0).await;
        });

        Ok(AsyncDB { shutdown, send })
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

    pub(crate) async fn run_server_async(
        mut db: DB,
        mut recv: impl ReceiverExt<Message> + Clone + 'static,
        mut shutdown: impl ReceiverExt<()> + Clone + 'static,
        mut snapshots: HashMap<usize, Snapshot>,
        mut snapshot_counter: usize,
    ) {
        if let Some(message) = recv.recv().await {
            Self::match_message(
                &mut db,
                recv.clone(),
                &mut snapshots,
                &mut snapshot_counter,
                message,
            );
        }

        spawn_local(async move {
            // check shutdown
            if let Some(()) = shutdown.recv().await {
                return;
            } else {
                AsyncDB::run_server_async(db, recv, shutdown, snapshots, snapshot_counter).await
            };
        });
    }

    pub(crate) async fn stop_server_async(&self) {
        self.shutdown.close();
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

impl ReceiverExt<Message> for channel::Receiver<Message> {
    fn blocking_recv(&mut self) -> Option<Message> {
        self.recv_blocking().ok()
    }

    fn close(&mut self) {
        channel::Receiver::close(self);
    }

    async fn recv(&mut self) -> Option<Message> {
        channel::Receiver::recv(&self).await.ok()
    }
}

impl ReceiverExt<()> for channel::Receiver<()> {
    fn blocking_recv(&mut self) -> Option<()> {
        self.recv_blocking().ok()
    }

    fn close(&mut self) {
        channel::Receiver::close(self);
    }

    async fn recv(&mut self) -> Option<()> {
        match channel::Receiver::try_recv(&self) {
            Ok(_) => Some(()),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Closed) => Some(()),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::{in_memory, AsyncDB};
    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    async fn test_asyncdb() {
        let db = AsyncDB::new("test.db", in_memory()).unwrap();
        db.put(b"key".to_vec(), b"value".to_vec()).await.unwrap();
        let val = db.get(b"key".to_vec()).await.unwrap();
        assert_eq!(val, Some(b"value".to_vec()));
        db.stop_server_async().await;
    }
}
