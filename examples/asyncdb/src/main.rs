use tokio::main;

use rusty_leveldb::{AsyncDB, Options};

#[main(flavor = "current_thread")]
async fn main() {
    let adb = AsyncDB::new("testdb", Options::default()).unwrap();

    assert!(adb.put("Hello".as_bytes().to_owned(), "World".as_bytes().to_owned()).await.is_ok());

    let r = adb.get("Hello".as_bytes().to_owned()).await;
    assert_eq!(r, Ok(Some("World".as_bytes().to_owned())));

    adb.flush().await.expect("flush()");
    adb.close().await.expect("close()");
}
