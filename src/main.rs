extern crate core;

use crate::blob::storage::Container;
use crate::config::{Args, Config};
use crate::metrics::Success::{No, Yes};
use crate::metrics::{HttpLabels, HttpMethod, HttpStatus};
use clap::Parser;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task;

mod blob;
mod config;
mod metrics;

const WRITER_COUNT: u32 = 10;

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

#[derive(Clone)]
struct Context {
    senders: Arc<RwLock<HashMap<u32, UnboundedSender<PostData>>>>,
    http_requests_metrics: Family<HttpLabels, Counter>,
    http_requests_registry: Arc<Registry>,
}

impl Context {
    pub fn new(senders: HashMap<u32, UnboundedSender<PostData>>) -> Self {
        let mut http_requests_registry = <Registry>::default();
        let http_requests_metrics = Family::<HttpLabels, Counter>::default();
        http_requests_registry.register(
            "http_requests",
            "Number of HTTP requests received",
            Box::new(http_requests_metrics.clone()),
        );
        Self {
            senders: Arc::new(RwLock::new(senders)),
            http_requests_metrics,
            http_requests_registry: Arc::new(http_requests_registry),
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Args = Args::parse();
    let config = Config::from_file(args.config)?;
    let mut senders = HashMap::new();

    for type_id in config.types {
        let (sender, mut receiver) = unbounded_channel();
        let type_id = type_id.clone();
        senders.insert(type_id.type_id, sender);
        task::spawn(async move {
            loop {
                let creation_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros();
                let mut container = Container::new(type_id.type_id);
                for _ in 0..type_id.objects_in_container {
                    let obj: PostData = receiver.recv().await.unwrap();
                    container.push(obj.writer_id, obj.data.as_slice());
                }
                let path = Path::new(type_id.root.as_str())
                    .join(format!("type{}_{}.blob", type_id.type_id, creation_time));
                println!("{}", path.to_str().unwrap());
                let file = File::create(path).unwrap();
                container.save_to_file(file).unwrap();
            }
        });
    }
    let ctx = Context::new(senders);
    let addr = ([0, 0, 0, 0], config.server.port).into();
    let service = make_service_fn(move |_| {
        let ctx = ctx.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |_req| {
                let ctx = ctx.clone();
                handler(_req, ctx)
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

async fn handler(req: Request<Body>, ctx: Context) -> Result<Response<Body>, hyper::Error> {
    match req.method() {
        &Method::POST => {
            let (type_id, writer_id) = match parse_path(req.uri().path()) {
                None => {
                    ctx.http_requests_metrics
                        .get_or_create(&HttpLabels {
                            method: HttpMethod::POST,
                            status: HttpStatus::Status2xx,
                            success: No,
                            type_id: 0,
                            writer_id: 0,
                        })
                        .inc();
                    return Ok(Response::new(Body::from(
                        r#"{ "state": -1,"reason"=41,desc="invalid path, need /type_id/N/writer_id/K" }"#
                            .to_string(),
                    )));
                }
                Some(type_id) => type_id,
            };

            if writer_id >= WRITER_COUNT {
                ctx.http_requests_metrics
                    .get_or_create(&HttpLabels {
                        method: HttpMethod::POST,
                        status: HttpStatus::Status2xx,
                        success: No,
                        type_id: 0,
                        writer_id: 0,
                    })
                    .inc();
                return Ok(Response::new(Body::from(
                    r#"{ "state": -1,"reason"=42,desc="invalid  writer_id value" }"#.to_string(),
                )));
            }
            let whole_body = hyper::body::to_bytes(req.into_body()).await?.to_vec();
            let senders = ctx.senders.read().unwrap();
            let sender = match senders.get(&type_id) {
                None => {
                    ctx.http_requests_metrics
                        .get_or_create(&HttpLabels {
                            method: HttpMethod::POST,
                            status: HttpStatus::Status2xx,
                            success: No,
                            type_id: 0,
                            writer_id,
                        })
                        .inc();
                    return Ok(Response::new(Body::from(
                        r#"{ "state": -1,"reason"=43,desc="invalid type_id value" }"#.to_string(),
                    )));
                }
                Some(s) => s.clone(),
            };

            sender.send(PostData::new(writer_id, whole_body)).unwrap();
            ctx.http_requests_metrics
                .get_or_create(&HttpLabels {
                    method: HttpMethod::POST,
                    status: HttpStatus::Status2xx,
                    success: Yes,
                    type_id,
                    writer_id,
                })
                .inc();
            Ok(Response::new(Body::from(r#"{ "state": 0 }"#.to_string())))
        }
        &Method::GET => {
            let mut buffer = vec![];
            encode(&mut buffer, &ctx.http_requests_registry).unwrap();
            Ok(Response::new(Body::from(buffer)))
        }
        _ => {
            ctx.http_requests_metrics
                .get_or_create(&HttpLabels {
                    method: HttpMethod::GET,
                    status: HttpStatus::Status4xx,
                    success: No,
                    type_id: 0,
                    writer_id: 0,
                })
                .inc();
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
