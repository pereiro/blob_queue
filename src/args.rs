use clap::Parser;

#[derive(Parser, Debug,Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Address:port to listen
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    pub listen: String,
    /// Root path to save blobs
    #[arg(short, long)]
    pub root: String,
}
