pub mod client;
pub mod observatory;

pub use {prost, tonic};

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/includes.rs"));
}
