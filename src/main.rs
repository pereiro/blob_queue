extern crate core;

use crate::args::Args;
use crate::blob::storage::Container;
use async_std::channel::{unbounded, Sender};
use async_std::task;
use clap::Parser;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tide::{Request, StatusCode};

mod args;
mod blob;

const TYPE_COUNT: u32 = 10;
const WRITER_ID: u32 = 1;
const OBJECTS_IN_CONTAINER: u32 = 1_000;

#[async_std::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let mut senders = Vec::new();

    for type_id in 1..TYPE_COUNT {
        let (sender, receiver) = unbounded();
        let root = args.root.clone();
        task::spawn(async move {
            let root = Path::new(root.as_str());
            let mut object_count = 0u64;
            loop {
                let mut container = Container::new(type_id);
                for _ in 0..OBJECTS_IN_CONTAINER {
                    let obj: Vec<u8> = receiver.recv().await.unwrap();
                    container.push(WRITER_ID, obj.as_slice());
                    object_count += 1;
                }
                let file = File::create(
                    root.with_file_name(format!("type{}_{}.blob", type_id, object_count)),
                )
                .unwrap();
                container.save_to_file(file).unwrap();
            }
        });
        senders.push(sender)
    }

    let senders = Arc::new(RwLock::new(senders));
    let mut http_server = tide::with_state(senders);
    http_server.at("/type_id/:type_id").post(push);
    http_server.listen(args.listen).await?;
    Ok(())
}

async fn push(mut req: Request<Arc<RwLock<Vec<Sender<Vec<u8>>>>>>) -> tide::Result {
    let type_id: u32 = req.param("type_id")?.parse::<u32>()?;
    if type_id <= 0 || type_id > TYPE_COUNT {
        return Err(tide::Error::from_str(
            StatusCode::BadRequest,
            "No such type_id, go away.",
        ));
    }
    let body = req.body_bytes().await?;
    let sender = req
        .state()
        .read()
        .unwrap()
        .get(type_id as usize)
        .unwrap()
        .clone();
    sender.send(body).await?;
    Ok(format!("OK").into())
}
