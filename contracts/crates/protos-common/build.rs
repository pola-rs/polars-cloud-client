fn main() -> Result<(), Box<dyn std::error::Error>> {
    build_deps::build(tonic_build::configure(), &["protos"])
}
