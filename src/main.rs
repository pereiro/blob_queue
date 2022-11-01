extern crate core;

use crate::args::Args;
use crate::blob::storage::Container;
use clap::Parser;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task;

mod args;
mod blob;

const TYPE_COUNT: u32 = 10;
const WRITER_COUNT: u32 = 10;
const OBJECTS_IN_CONTAINER: u32 = 1_000;

#[derive(Debug)]
struct PostData {
    data: Vec<u8>,
    writer_id: u32,
}

impl PostData {
    fn new(writer_id: u32, data: Vec<u8>) -> Self {
        Self { data, writer_id }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Args = Args::parse();
    let mut senders = Vec::new();

    println!("root = {}", args.root);

    for type_id in 0..TYPE_COUNT - 1 {
        let (sender, mut receiver) = unbounded_channel();
        senders.push(sender);
        let root = args.root.clone();
        task::spawn(async move {
            loop {
                let creation_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros();
                let mut container = Container::new(type_id);
                for _ in 0..OBJECTS_IN_CONTAINER {
                    let obj: PostData = receiver.recv().await.unwrap();
                    container.push(obj.writer_id, obj.data.as_slice());
                }
                let path = Path::new(root.as_str())
                    .join(format!("type{}_{}.blob", type_id, creation_time));
                println!("{}", path.to_str().unwrap());
                let file = File::create(path).unwrap();
                container.save_to_file(file).unwrap();
            }
        });
    }
    let senders = Arc::new(RwLock::new(senders));
    let addr = ([0, 0, 0, 0], args.port).into();
    let service = make_service_fn(move |_| {
        let senders = senders.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |_req| {
                let senders = senders.clone();
                handler(_req, senders)
            }))
        }
    });

    let server = Server::bind(&addr).serve(service);

    println!("Listening {}", addr);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }

    Ok(())
}

async fn handler(
    req: Request<Body>,
    senders: Arc<RwLock<Vec<UnboundedSender<PostData>>>>,
) -> Result<Response<Body>, hyper::Error> {
    let (type_id, writer_id) = match parse_path(req.uri().path()) {
        None => {
            return Ok(Response::new(Body::from(
                r#"{ "state": -1,"reason"=41,desc="invalid path, need /type_id/N/writer_id/K" }"#
                    .to_string(),
            )))
        }
        Some(type_id) => type_id,
    };
    if type_id >= TYPE_COUNT || writer_id >= WRITER_COUNT {
        return Ok(Response::new(Body::from(
            r#"{ "state": -1,"reason"=42,desc="invalid type_id or writer_id value" }"#.to_string(),
        )));
    }
    match req.method() {
        &Method::POST => {
            let whole_body = hyper::body::to_bytes(req.into_body()).await?.to_vec();
            let senders = senders.read().unwrap();
            let sender = senders.get(type_id as usize).unwrap();
            sender.send(PostData::new(writer_id, whole_body)).unwrap();
            Ok(Response::new(Body::from(r#"{ "state": 0 }"#.to_string())))
        }
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
            Ok(not_found)
        }
    }
}

fn parse_path(path: &str) -> Option<(u32, u32)> {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 5
        || parts[1].to_lowercase() != "type_id"
        || parts[3].to_lowercase() != "writer_id"
    {
        return None;
    }
    let type_id = match parts[2].parse::<u32>() {
        Ok(type_id) => type_id,
        Err(_) => return None,
    };
    let writer_id = match parts[4].parse::<u32>() {
        Ok(writer_id) => writer_id,
        Err(_) => return None,
    };
    Some((type_id, writer_id))
}
