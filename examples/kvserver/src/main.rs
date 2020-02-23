use canteen;
use rusty_leveldb;

struct KVService {
    db: rusty_leveldb::DB,
}

static mut STORAGE_SERVICE: Option<std::sync::Mutex<KVService>> = None;

impl KVService {
    fn handle_get(&mut self, req: &canteen::Request) -> canteen::Response {
        let key: String = req.get("key");

        let val = self.db.get(key.as_bytes());

        let mut rp = canteen::Response::new();

        rp.set_status(200);
        rp.set_content_type("text/plain");

        if let Some(val) = val {
            rp.append(val);
        } else {
            rp.set_status(404);
        }
        rp
    }
    fn handle_put(&mut self, req: &canteen::Request) -> canteen::Response {
        let mut rp = canteen::Response::new();
        let key: String = req.get("key");
        let val = &req.payload;

        self.db.put(key.as_bytes(), val.as_ref()).unwrap();

        rp.set_status(200);
        rp.set_content_type("text/plain");
        rp
    }
}

fn get_key_fn(rq: &canteen::Request) -> canteen::Response {
    unsafe {
        STORAGE_SERVICE
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .handle_get(rq)
    }
}

fn put_key_fn(rq: &canteen::Request) -> canteen::Response {
    unsafe {
        STORAGE_SERVICE
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .handle_put(rq)
    }
}

fn main() {
    let db = rusty_leveldb::DB::open("httpdb", rusty_leveldb::Options::default()).unwrap();
    let service = KVService { db: db };
    unsafe { STORAGE_SERVICE = Some(std::sync::Mutex::new(service)) };

    let mut ct = canteen::Canteen::new();
    ct.add_route("/kvs/get/<str:key>", &[canteen::Method::Get], get_key_fn);
    ct.add_route(
        "/kvs/put/<str:key>",
        &[canteen::Method::Put, canteen::Method::Post],
        put_key_fn,
    );
    ct.bind("0.0.0.0:8080");
    ct.run()
}
