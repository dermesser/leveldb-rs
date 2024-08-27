use std::collections::hash_map::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::{Options, Result, Status, StatusCode, WriteBatch, DB};

#[cfg(feature = "asyncdb-tokio")]
use tokio::sync::mpsc;
#[cfg(feature = "asyncdb-tokio")]
use tokio::sync::oneshot;
#[cfg(feature = "asyncdb-tokio")]
use tokio::task::{spawn_blocking, JoinHandle};

#[cfg(feature = "asyncdb-async-std")]
use async_std::channel;
#[cfg(feature = "asyncdb-async-std")]
use async_std::task::{spawn_blocking, JoinHandle};

const CHANNEL_BUFFER_SIZE: usize = 32;

#[derive(Clone, Copy)]
pub struct SnapshotRef(usize);

/// A request sent to the database thread.
enum Request {
    Close,
    Put { key: Vec<u8>, val: Vec<u8> },
    Delete { key: Vec<u8> },
    Write { batch: WriteBatch, sync: bool },
    Flush,
    GetAt { snapshot: SnapshotRef, key: Vec<u8> },
    Get { key: Vec<u8> },
    GetSnapshot,
    DropSnapshot { snapshot: SnapshotRef },
    CompactRange { from: Vec<u8>, to: Vec<u8> },
}

/// A response received from the database thread.
enum Response {
    OK,
    Error(Status),
    Value(Option<Vec<u8>>),
    Snapshot(SnapshotRef),
}

/// Contains both a request and a back-channel for the reply.
struct Message {
    req: Request,
    #[cfg(feature = "asyncdb-tokio")]
    resp_channel: oneshot::Sender<Response>,
    #[cfg(feature = "asyncdb-async-std")]
    resp_channel: channel::Sender<Response>,
}

/// `AsyncDB` makes it easy to use LevelDB in a tokio runtime.
/// The methods follow very closely the main API (see `DB` type). Iteration is not yet implemented.
///
/// TODO: Make it work in other runtimes as well. This is a matter of adapting the blocking thread
/// mechanism as well as the channel types.
#[derive(Clone)]
pub struct AsyncDB {
    jh: Arc<JoinHandle<()>>,
    #[cfg(feature = "asyncdb-tokio")]
    send: mpsc::Sender<Message>,
    #[cfg(feature = "asyncdb-async-std")]
    send: channel::Sender<Message>,
}

impl AsyncDB {
    /// Create a new or open an existing database.
    pub fn new<P: AsRef<Path>>(name: P, opts: Options) -> Result<AsyncDB> {
        let db = DB::open(name, opts)?;
        #[cfg(feature = "asyncdb-tokio")]
        let (send, recv) = mpsc::channel(CHANNEL_BUFFER_SIZE);

        #[cfg(feature = "asyncdb-async-std")]
        let (send, recv) = channel::bounded(CHANNEL_BUFFER_SIZE);

        let jh = spawn_blocking(move || AsyncDB::run_server(db, recv));
        Ok(AsyncDB {
            jh: Arc::new(jh),
            send,
        })
    }

    pub async fn close(&self) -> Result<()> {
        let r = self.process_request(Request::Close).await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status {
                code: StatusCode::AsyncError,
                err: "Wrong response type in AsyncDB.".to_string(),
            }),
        }
    }

    pub async fn put(&self, key: Vec<u8>, val: Vec<u8>) -> Result<()> {
        let r = self.process_request(Request::Put { key, val }).await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status {
                code: StatusCode::AsyncError,
                err: "Wrong response type in AsyncDB.".to_string(),
            }),
        }
    }
    pub async fn delete(&self, key: Vec<u8>) -> Result<()> {
        let r = self.process_request(Request::Delete { key }).await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status {
                code: StatusCode::AsyncError,
                err: "Wrong response type in AsyncDB.".to_string(),
            }),
        }
    }
    pub async fn write(&self, batch: WriteBatch, sync: bool) -> Result<()> {
        let r = self.process_request(Request::Write { batch, sync }).await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status {
                code: StatusCode::AsyncError,
                err: "Wrong response type in AsyncDB.".to_string(),
            }),
        }
    }
    pub async fn flush(&self) -> Result<()> {
        let r = self.process_request(Request::Flush).await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status {
                code: StatusCode::AsyncError,
                err: "Wrong response type in AsyncDB.".to_string(),
            }),
        }
    }
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let r = self.process_request(Request::Get { key }).await?;
        match r {
            Response::Value(v) => Ok(v),
            Response::Error(s) => Err(s),
            _ => Err(Status {
                code: StatusCode::AsyncError,
                err: "Wrong response type in AsyncDB.".to_string(),
            }),
        }
    }
    pub async fn get_at(&self, snapshot: SnapshotRef, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let r = self
            .process_request(Request::GetAt { snapshot, key })
            .await?;
        match r {
            Response::Value(v) => Ok(v),
            Response::Error(s) => Err(s),
            _ => Err(Status {
                code: StatusCode::AsyncError,
                err: "Wrong response type in AsyncDB.".to_string(),
            }),
        }
    }
    pub async fn get_snapshot(&self) -> Result<SnapshotRef> {
        let r = self.process_request(Request::GetSnapshot).await?;
        match r {
            Response::Snapshot(sr) => Ok(sr),
            _ => Err(Status {
                code: StatusCode::AsyncError,
                err: "Wrong response type in AsyncDB.".to_string(),
            }),
        }
    }
    /// As snapshots returned by `AsyncDB::get_snapshot()` are sort-of "weak references" to an
    /// actual snapshot, they need to be dropped explicitly.
    pub async fn drop_snapshot(&self, snapshot: SnapshotRef) -> Result<()> {
        let r = self
            .process_request(Request::DropSnapshot { snapshot })
            .await?;
        match r {
            Response::OK => Ok(()),
            _ => Err(Status {
                code: StatusCode::AsyncError,
                err: "Wrong response type in AsyncDB.".to_string(),
            }),
        }
    }
    pub async fn compact_range(&self, from: Vec<u8>, to: Vec<u8>) -> Result<()> {
        let r = self
            .process_request(Request::CompactRange { from, to })
            .await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status {
                code: StatusCode::AsyncError,
                err: "Wrong response type in AsyncDB.".to_string(),
            }),
        }
    }

    async fn process_request(&self, req: Request) -> Result<Response> {
        #[cfg(feature = "asyncdb-tokio")]
        let (tx, rx) = oneshot::channel();

        #[cfg(feature = "asyncdb-async-std")]
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
        #[cfg(feature = "asyncdb-tokio")]
        let resp = rx.await;

        #[cfg(feature = "asyncdb-async-std")]
        let resp = rx.recv().await;
        match resp {
            Err(e) => Err(Status {
                code: StatusCode::AsyncError,
                err: e.to_string(),
            }),
            Ok(r) => Ok(r),
        }
    }

    fn _run_server(mut db: DB, mut recv: impl ReceiverExt<Message>) {
        {
            let mut snapshots = HashMap::new();
            let mut snapshot_counter: usize = 0;

            while let Some(message) = recv.blocking_recv() {
                match message.req {
                    Request::Close => {
                        send_response(message.resp_channel, Response::OK);
                        recv.close();
                        return;
                    }
                    Request::Put { key, val } => {
                        let ok = db.put(&key, &val);
                        send_response_result(message.resp_channel, ok);
                    }
                    Request::Delete { key } => {
                        let ok = db.delete(&key);
                        send_response_result(message.resp_channel, ok);
                    }
                    Request::Write { batch, sync } => {
                        let ok = db.write(batch, sync);
                        send_response_result(message.resp_channel, ok);
                    }
                    Request::Flush => {
                        let ok = db.flush();
                        send_response_result(message.resp_channel, ok);
                    }
                    Request::GetAt { snapshot, key } => {
                        let snapshot_id = snapshot.0;
                        if let Some(snapshot) = snapshots.get(&snapshot_id) {
                            let ok = db.get_at(snapshot, &key);
                            match ok {
                                Err(e) => {
                                    send_response(message.resp_channel, Response::Error(e));
                                }
                                Ok(v) => {
                                    send_response(message.resp_channel, Response::Value(v));
                                }
                            };
                        } else {
                            send_response(
                                message.resp_channel,
                                Response::Error(Status {
                                    code: StatusCode::AsyncError,
                                    err: "Unknown snapshot reference: this is a bug".to_string(),
                                }),
                            );
                        }
                    }
                    Request::Get { key } => {
                        let r = db.get(&key);
                        send_response(message.resp_channel, Response::Value(r));
                    }
                    Request::GetSnapshot => {
                        snapshots.insert(snapshot_counter, db.get_snapshot());
                        let sref = SnapshotRef(snapshot_counter);
                        snapshot_counter += 1;
                        send_response(message.resp_channel, Response::Snapshot(sref));
                    }
                    Request::DropSnapshot { snapshot } => {
                        snapshots.remove(&snapshot.0);
                        send_response_result(message.resp_channel, Ok(()));
                    }
                    Request::CompactRange { from, to } => {
                        let ok = db.compact_range(&from, &to);
                        send_response_result(message.resp_channel, ok);
                    }
                }
            }
        }
    }

    #[cfg(feature = "asyncdb-tokio")]
    fn run_server(db: DB, recv: mpsc::Receiver<Message>) {
        Self::_run_server(db, recv);
    }

    #[cfg(feature = "asyncdb-async-std")]
    fn run_server(db: DB, recv: channel::Receiver<Message>) {
        Self::_run_server(db, recv);
    }
}

#[cfg(feature = "asyncdb-tokio")]
fn send_response_result(ch: oneshot::Sender<Response>, result: Result<()>) {
    if let Err(e) = result {
        ch.send(Response::Error(e)).ok();
    } else {
        ch.send(Response::OK).ok();
    }
}

#[cfg(feature = "asyncdb-async-std")]
fn send_response_result(ch: channel::Sender<Response>, result: Result<()>) {
    if let Err(e) = result {
        ch.try_send(Response::Error(e)).ok();
    } else {
        ch.try_send(Response::OK).ok();
    }
}

#[cfg(feature = "asyncdb-tokio")]
fn send_response(ch: oneshot::Sender<Response>, res: Response) {
    ch.send(res).ok();
}

#[cfg(feature = "asyncdb-async-std")]
fn send_response(ch: channel::Sender<Response>, res: Response) {
    ch.send_blocking(res).ok();
}

trait ReceiverExt<T> {
    fn blocking_recv(&mut self) -> Option<T>;
    fn close(&mut self);
}

#[cfg(feature = "asyncdb-tokio")]
impl<T> ReceiverExt<T> for mpsc::Receiver<T> {
    fn blocking_recv(&mut self) -> Option<T> {
        self.blocking_recv()
    }

    fn close(&mut self) {
        mpsc::Receiver::close(self);
    }
}

#[cfg(feature = "asyncdb-async-std")]
impl<T> ReceiverExt<T> for channel::Receiver<T> {
    fn blocking_recv(&mut self) -> Option<T> {
        self.recv_blocking().ok()
    }

    fn close(&mut self) {
        channel::Receiver::close(self);
    }
}
