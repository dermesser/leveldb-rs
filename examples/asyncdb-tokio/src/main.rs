use tokio::main;

use rusty_leveldb::{AsyncDB, Options, Status, StatusCode};

#[main(flavor = "current_thread")]
async fn main() {
    let adb = AsyncDB::new("testdb", Options::default()).unwrap();

    adb.put("Hello".as_bytes().to_owned(), "World".as_bytes().to_owned())
        .await
        .expect("put()");

    let r = adb.get("Hello".as_bytes().to_owned()).await;
    assert_eq!(r, Ok(Some("World".as_bytes().to_owned())));

    let snapshot = adb.get_snapshot().await.expect("get_snapshot()");

    adb.delete("Hello".as_bytes().to_owned())
        .await
        .expect("delete()");

    // A snapshot allows us to travel back in time before the deletion.
    let r2 = adb.get_at(snapshot, "Hello".as_bytes().to_owned()).await;
    assert_eq!(r2, Ok(Some("World".as_bytes().to_owned())));

    // Once dropped, a snapshot cannot be used anymore.
    adb.drop_snapshot(snapshot).await.expect("drop_snapshot()");

    let r3 = adb.get_at(snapshot, "Hello".as_bytes().to_owned()).await;
    assert_eq!(
        r3,
        Err(Status {
            code: StatusCode::AsyncError,
            err: "Unknown snapshot reference: this is a bug".to_string()
        })
    );

    adb.flush().await.expect("flush()");
    adb.close().await.expect("close()");
}
