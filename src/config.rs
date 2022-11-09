use clap::Parser;
use serde_derive::Deserialize;
use std::fs::File;
use std::io;
use std::io::{ErrorKind, Read};
use std::path::Path;

#[derive(Deserialize)]
pub struct Config {
    pub server: HttpConfig,
    pub types: Vec<TypeConfig>,
}

#[derive(Deserialize, Clone)]
pub struct HttpConfig {
    pub port: u16,
}

#[derive(Deserialize, Clone)]
pub struct TypeConfig {
    pub type_id: u32,
    pub root: String,
    pub objects_in_container: u32,
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// path to config
    #[arg(short, long)]
    pub config: String,
}

impl Config {
    pub fn from_file(path: String) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut toml_str = "".to_string();
        file.read_to_string(&mut toml_str)?;
        match toml::from_str::<Self>(toml_str.as_str()) {
            Ok(config) => config.check_config(),
            Err(err) => Err(io::Error::new(ErrorKind::Unsupported, err)),
        }
    }
    pub fn check_config(self) -> io::Result<Self> {
        for type_id in self.types.iter() {
            let path = Path::new(type_id.root.as_str());
            if !path.exists() {
                return Err(io::Error::new(
                    ErrorKind::NotFound,
                    format!("path {} not found", type_id.root),
                ));
            }
            if !path.is_dir() {
                return Err(io::Error::new(
                    ErrorKind::NotFound,
                    format!("path {} is not a directory", type_id.root),
                ));
            }
        }
        Ok(self)
    }
}
