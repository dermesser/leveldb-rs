
use std::path::Path;

use crate::{DB, Status, StatusCode, Options, Result, snapshot::Snapshot, WriteBatch};

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, spawn_blocking};

const CHANNEL_BUFFER_SIZE: usize = 32;

enum Request {
    Close,
    Put { key: Vec<u8>, val: Vec<u8> },
    Delete { key: Vec<u8> },
    Write { batch: WriteBatch, sync: bool },
    Flush,
    //GetAt { snapshot: Snapshot, key: Vec<u8> },
    Get { key: Vec<u8> },
    //GetSnapshot,
    CompactRange { from: Vec<u8>, to: Vec<u8> }
}

enum Response {
    OK,
    Error(Status),
    Value(Option<Vec<u8>>),
    // Idea: don't send snapshots but opaque reference to a snapshot that doesn't leave the worker
    // thread.
    //Snapshot(Snapshot),
}

struct Message {
    req: Request,
    resp_channel: oneshot::Sender<Response>,
}

pub struct AsyncDB {
    jh: JoinHandle<()>,
    send: mpsc::Sender<Message>,
}

impl AsyncDB {

    pub fn new<P: AsRef<Path>>(name: P, opts: Options) -> Result<AsyncDB> {
        let db = DB::open(name, opts)?;
        let (send, recv) = mpsc::channel(CHANNEL_BUFFER_SIZE);
        let jh = spawn_blocking(move || AsyncDB::run_server(db, recv));
        Ok(AsyncDB { jh, send })
    }

    pub async fn close(&self) -> Result<()> {
        let r = self.process_request(Request::Close).await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status { code: StatusCode::AsyncError, err: "Wrong response type in AsyncDB.".to_string() }),
        }
    }

    pub async fn put(&self, key: Vec<u8>, val: Vec<u8>) -> Result<()> {
        let r = self.process_request(Request::Put{key, val}).await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status { code: StatusCode::AsyncError, err: "Wrong response type in AsyncDB.".to_string() }),
        }
    }
    pub async fn delete(&self, key: Vec<u8>) -> Result<()> {
        let r = self.process_request(Request::Delete{key}).await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status { code: StatusCode::AsyncError, err: "Wrong response type in AsyncDB.".to_string() }),
        }
    }
    pub async fn write(&self, batch: WriteBatch, sync: bool) -> Result<()> {
        let r = self.process_request(Request::Write{batch, sync}).await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status { code: StatusCode::AsyncError, err: "Wrong response type in AsyncDB.".to_string() }),
        }
    }
    pub async fn flush(&self) -> Result<()> {
        let r = self.process_request(Request::Flush).await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status { code: StatusCode::AsyncError, err: "Wrong response type in AsyncDB.".to_string() }),
        }
    }
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let r = self.process_request(Request::Get { key }).await?;
        match r {
            Response::Value(v) => Ok(v),
            Response::Error(s) => Err(s),
            _ => Err(Status { code: StatusCode::AsyncError, err: "Wrong response type in AsyncDB.".to_string() }),
        }
    }
    pub async fn compact_range(&self, from: Vec<u8>, to: Vec<u8>) -> Result<()> {
        let r = self.process_request(Request::CompactRange { from, to }).await?;
        match r {
            Response::OK => Ok(()),
            Response::Error(s) => Err(s),
            _ => Err(Status { code: StatusCode::AsyncError, err: "Wrong response type in AsyncDB.".to_string() }),
        }
    }

    async fn process_request(&self, req: Request) -> Result<Response> {
        let (tx, rx) = oneshot::channel();
        let m = Message { req, resp_channel: tx };
        if let Err(e) = self.send.send(m).await {
            return Err(Status { code: StatusCode::AsyncError, err: e.to_string() });
        }
        let resp = rx.await;
        match resp {
            Err(e) => Err(Status { code: StatusCode::AsyncError, err: e.to_string() }),
            Ok(r) => Ok(r),
        }
    }

    fn run_server(mut db: DB, mut recv: mpsc::Receiver<Message>) {
        while let Some(message) = recv.blocking_recv() {
            match message.req {
                Request::Close => {
                    message.resp_channel.send(Response::OK).ok();
                    recv.close();
                    return;
                },
                Request::Put { key, val } => {
                    let ok = db.put(&key, &val);
                    send_response(message.resp_channel, ok);
                },
                Request::Delete { key } => {
                    let ok = db.delete(&key);
                    send_response(message.resp_channel, ok);
                },
                Request::Write { batch, sync } => {
                    let ok = db.write(batch, sync);
                    send_response(message.resp_channel, ok);
                },
                Request::Flush => {
                    let ok = db.flush();
                    send_response(message.resp_channel, ok);
                },
                /*Request::GetAt { snapshot, key } => {
                    let ok = db.get_at(&snapshot, &key);
                    match ok {
                        Err(e) => {
                            message.resp_channel.send(Response::Error(e));
                        },
                        Ok(v) => {
                            message.resp_channel.send(Response::Value(v));
                        }
                    };
                },
                */
                Request::Get { key } => {
                    let r = db.get(&key);
                    message.resp_channel.send(Response::Value(r));
                },
                /*Request::GetSnapshot => {
                    message.resp_channel.send(Response::Snapshot(db.get_snapshot()));
                },
                */
                Request::CompactRange { from, to } => {
                    let ok = db.compact_range(&from, &to);
                    send_response(message.resp_channel, ok);
                }
            }
        }
    }
}

fn send_response(ch: oneshot::Sender<Response>, result: Result<()>) {
    if let Err(e) = result {
        ch.send(Response::Error(e));
    } else {
        ch.send(Response::OK);
    }
}

