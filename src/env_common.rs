use std::thread;
use std::time;

pub fn micros() -> u64 {
    loop {
        let now = time::SystemTime::now().duration_since(time::UNIX_EPOCH);

        match now {
            Err(_) => continue,
            Ok(dur) => return dur.as_secs() * 1000000 + dur.subsec_micros() as u64,
        }
    }
}

pub fn sleep_for(micros: u32) {
    thread::sleep(time::Duration::new(0, micros * 1000));
}
