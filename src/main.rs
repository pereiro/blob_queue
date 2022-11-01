extern crate core;

use crate::args::Args;
use crate::blob::storage::Container;
use clap::Parser;
use std::fs::File;
use std::num::ParseIntError;
use std::path::Path;
use std::sync::{Arc, mpsc, RwLock};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use futures_util::TryStreamExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task;


mod args;
mod blob;

const TYPE_COUNT: u32 = 10;
const WRITER_ID: u32 = 1;
const OBJECTS_IN_CONTAINER: u32 = 1_000;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Args = Args::parse();
    let mut senders = Vec::new();

    println!("root = {}",args.root);

    for type_id in 0..TYPE_COUNT-1 {
        let (sender, mut receiver) = unbounded_channel();
        senders.push(sender);
        let root = args.root.clone();
        task::spawn(async move {
            let mut object_count = 0u64;
            loop {
                let mut container = Container::new(type_id);
                for _ in 0..OBJECTS_IN_CONTAINER {
                    let obj: Vec<u8> = receiver.recv().await.unwrap();
                    container.push(WRITER_ID, obj.as_slice());
                    object_count += 1;
                }
                let path = Path::new(root.as_str()).with_file_name(format!("type{}_{}.blob", type_id, object_count));
                println!("{}",path.to_str().unwrap());
                let file = File::create(path).unwrap();
                container.save_to_file(file).unwrap();
            }
        });
    }
    let senders = Arc::new(RwLock::new(senders));
    let addr = ([0, 0, 0, 0], 8080).into();
    let service = make_service_fn(move |_| {
        let mut senders = senders.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |_req|{
                let senders = senders.clone();
                handler(_req,senders)
            }))
        }
    });

    let server = Server::bind(&addr).serve(service);

    println!("Listening on http://{}", addr);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }

    Ok(())
}

async fn handler(req: Request<Body>,senders:Arc<RwLock<Vec<UnboundedSender<Vec<u8>>>>>) -> Result<Response<Body>, hyper::Error> {
    let type_id = match  parse_type_id(req.uri().path()) {
        None => {
            let mut bad_request = Response::default();
            *bad_request.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(bad_request)
        }
        Some(type_id ) => { type_id}
    };
    if type_id >= TYPE_COUNT {
        let mut bad_request = Response::default();
        *bad_request.status_mut() = StatusCode::BAD_REQUEST;
        return Ok(bad_request)
    }
    match req.method() {
        (&Method::POST) => {
            let whole_body = hyper::body::to_bytes(req.into_body()).await?.to_vec();
            let senders = senders.read().unwrap();
            let sender = senders.get(type_id as usize ).unwrap();
            sender.send(whole_body).unwrap();
            Ok(Response::new(Body::from(format!("OK,type_id={}",type_id))))
        }
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

fn parse_type_id(path: &str) -> Option<u32>{
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len()<3 || parts[1].to_lowercase() != "type_id"{
        return None;
    }
    match parts[2].parse::<u32>(){
        Ok(type_id) => { Some(type_id)}
        Err(_) => { None }
    }
}