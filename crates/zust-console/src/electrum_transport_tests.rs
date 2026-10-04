use super::*;
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;

fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let source = format!("tcp://{}", listener.local_addr().unwrap());
    (listener, source)
}

fn accept(listener: &TcpListener) -> BufReader<TcpStream> {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).unwrap();
                stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                stream.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
                return BufReader::new(stream);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < until, "mock Electrum accept timed out");
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => panic!("mock accept failed: {error}"),
        }
    }
}

fn request(reader: &mut BufReader<TcpStream>) -> Value {
    let mut line = String::new();
    assert!(reader.read_line(&mut line).unwrap() > 0, "request missing");
    serde_json::from_str(&line).unwrap()
}

fn reply(reader: &mut BufReader<TcpStream>, request: &Value, value: Value) {
    writeln!(reader.get_mut(), "{}", json!({"id": request["id"], "result": value})).unwrap();
}

fn call(source: &str, method: &str) -> Result<Value> {
    rpc(source, method, json!([]), Duration::from_secs(1),
        Instant::now() + Duration::from_secs(3))
}

#[test]
fn single_and_pipeline_share_connection_with_reversed_replies_and_notifications() {
    let (listener, source) = listener();
    let server = thread::spawn(move || {
        let mut stream = accept(&listener);
        let first = request(&mut stream);
        reply(&mut stream, &first, json!(1));
        let a = request(&mut stream);
        let b = request(&mut stream);
        assert_ne!(a["id"], b["id"]);
        assert_ne!(a["id"], first["id"]);
        writeln!(stream.get_mut(), "{}", json!({"method":"blockchain.headers.subscribe","params":[]})).unwrap();
        reply(&mut stream, &b, json!(22));
        reply(&mut stream, &a, json!(11));
        let last = request(&mut stream);
        reply(&mut stream, &last, json!(3));
        assert!(matches!(listener.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock));
    });
    assert_eq!(call(&source, "blockchain.headers.subscribe").unwrap(), json!(1));
    let calls = [(7, "blockchain.scripthash.listunspent".into(), json!(["a"])),
                 (8, "blockchain.scripthash.listunspent".into(), json!(["b"]))];
    let results = pipeline(&source.replacen("tcp://", "electrum://", 1), &calls,
        Duration::from_secs(1), Instant::now() + Duration::from_secs(3)).unwrap();
    assert_eq!(results[&7], Ok(json!(11)));
    assert_eq!(results[&8], Ok(json!(22)));
    assert_eq!(call(&source, "blockchain.transaction.get").unwrap(), json!(3));
    server.join().unwrap();
}

#[test]
fn disconnect_drops_connection_without_replaying_broadcast() {
    let (listener, source) = listener();
    let server = thread::spawn(move || {
        let mut first = accept(&listener);
        let broadcast = request(&mut first);
        assert_eq!(broadcast["method"], "blockchain.transaction.broadcast");
        drop(first);
        let mut second = accept(&listener);
        let query = request(&mut second);
        assert_eq!(query["method"], "blockchain.transaction.get", "broadcast must never be replayed");
        reply(&mut second, &query, json!("reconnected"));
    });
    let error = call(&source, "blockchain.transaction.broadcast").unwrap_err();
    assert!(format!("{error:#}").contains("blockchain.transaction.broadcast"));
    assert_eq!(call(&source, "blockchain.transaction.get").unwrap(), json!("reconnected"));
    server.join().unwrap();
}

#[test]
fn timeout_discards_socket_and_late_reply_cannot_satisfy_next_request() {
    let (listener, source) = listener();
    let server = thread::spawn(move || {
        let mut first = accept(&listener);
        let old = request(&mut first);
        thread::sleep(Duration::from_millis(250));
        let _ = writeln!(first.get_mut(), "{}", json!({"id":old["id"],"result":"stale"}));
        let mut second = accept(&listener);
        let new = request(&mut second);
        reply(&mut second, &new, json!("fresh"));
    });
    assert!(rpc(&source, "blockchain.transaction.get", json!([]), Duration::from_secs(1),
        Instant::now() + Duration::from_millis(150)).is_err());
    assert_eq!(call(&source, "blockchain.transaction.get").unwrap(), json!("fresh"));
    server.join().unwrap();
}

#[test]
fn unexpected_response_id_discards_socket() {
    let (listener, source) = listener();
    let server = thread::spawn(move || {
        let mut first = accept(&listener);
        let req = request(&mut first);
        writeln!(first.get_mut(), "{}", json!({"id":req["id"].as_u64().unwrap()+1000000,"result":"wrong"})).unwrap();
        let mut second = accept(&listener);
        let req = request(&mut second);
        reply(&mut second, &req, json!("correct"));
    });
    assert!(format!("{:#}", call(&source, "blockchain.transaction.get").unwrap_err())
        .contains("unexpected or duplicate"));
    assert_eq!(call(&source, "blockchain.transaction.get").unwrap(), json!("correct"));
    server.join().unwrap();
}

#[test]
fn rpc_error_does_not_poison_a_fully_drained_connection() {
    let (listener, source) = listener();
    let server = thread::spawn(move || {
        let mut stream = accept(&listener);
        let req = request(&mut stream);
        writeln!(stream.get_mut(), "{}", json!({"id":req["id"],"error":{"code":-1,"message":"not found"}})).unwrap();
        let req = request(&mut stream);
        reply(&mut stream, &req, json!("ok"));
    });
    assert!(format!("{:#}", call(&source, "blockchain.transaction.get").unwrap_err()).contains("not found"));
    assert_eq!(call(&source, "blockchain.transaction.get").unwrap(), json!("ok"));
    server.join().unwrap();
}

#[test]
fn connection_cap_waits_with_deadline_and_releases_failed_leases() {
    let (listener, source) = listener();
    let (release, released) = mpsc::channel();
    let server = thread::spawn(move || {
        let sockets = (0..MAX_CONNECTIONS).map(|_| accept(&listener)).collect::<Vec<_>>();
        released.recv_timeout(Duration::from_secs(3)).unwrap();
        let final_socket = accept(&listener);
        drop((sockets, final_socket));
    });
    let endpoint = endpoint(&source).unwrap();
    let mut leases = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        leases.push(checkout(&endpoint, Duration::from_secs(1),
            Instant::now() + Duration::from_secs(2)).unwrap());
    }
    assert!(checkout(&endpoint, Duration::from_secs(1),
        Instant::now() + Duration::from_millis(50)).is_err());
    drop(leases);
    release.send(()).unwrap();
    drop(checkout(&endpoint, Duration::from_secs(1),
        Instant::now() + Duration::from_secs(2)).unwrap());
    server.join().unwrap();
}

#[test]
fn partial_timeout_preserves_completed_results_but_discards_connection() {
    let (listener, source) = listener();
    let server = thread::spawn(move || {
        let mut first = accept(&listener);
        let done = request(&mut first);
        let late = request(&mut first);
        reply(&mut first, &done, json!("done"));
        thread::sleep(Duration::from_millis(250));
        let _ = writeln!(first.get_mut(), "{}", json!({"id":late["id"],"result":"late"}));
        let mut second = accept(&listener);
        let fresh = request(&mut second);
        reply(&mut second, &fresh, json!("fresh"));
    });
    let calls = [(10, "blockchain.scripthash.listunspent".into(), json!(["one"])),
                 (20, "blockchain.scripthash.listunspent".into(), json!(["two"]))];
    let result = pipeline_partial(&source, &calls, Duration::from_secs(1),
        Instant::now() + Duration::from_millis(150)).unwrap();
    assert_eq!(result[&10], Ok(json!("done")));
    assert!(result[&20].is_err());
    assert_eq!(call(&source, "blockchain.transaction.get").unwrap(), json!("fresh"));
    server.join().unwrap();
}
