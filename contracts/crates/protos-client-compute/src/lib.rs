pub mod client;

pub use prost;
pub use tonic;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/includes.rs"));
}
