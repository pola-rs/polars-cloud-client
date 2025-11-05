pub mod control;

pub use control::*;
pub use {prost, tonic};

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/includes.rs"));
}

// gRPC default is 4 MiB - we set the maximum to be a bit more lenient to
// facilitate sending plans containing a small amount of data.
// Make sure this matches the maximum message length set in the client / control plane.
pub const MAX_MESSAGE_LENGTH_CONTROL_PLANE: usize = 10 * 1024 * 1024;
