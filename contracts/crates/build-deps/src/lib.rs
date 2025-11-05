use std::ffi::OsStr;
use std::path::{Path, PathBuf};

fn visit_files(path: &Path) -> impl Iterator<Item = PathBuf> + use<> {
    path.read_dir()
        .unwrap()
        .flat_map(|entry| -> Box<dyn Iterator<Item = PathBuf>> {
            let entry = entry.unwrap();
            let filetype = entry.file_type().unwrap();
            if filetype.is_dir() {
                Box::new(visit_files(&entry.path()))
            } else if filetype.is_file() && entry.path().extension() == Some(OsStr::new("proto")) {
                Box::new(std::iter::once(entry.path()))
            } else {
                Box::new(std::iter::empty())
            }
        })
}

pub fn build_with_common() -> Result<(), Box<dyn std::error::Error>> {
    build(
        tonic_build::configure().extern_path(
            ".polars_cloud.common",
            "::protos_common::proto::polars_cloud::common",
        ),
        &["protos", "../protos-common/protos"],
    )
}

pub fn build(
    builder: tonic_build::Builder,
    includes: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config
        .btree_map(["polars_cloud.compute_plane.client.v1.QueryStageStatistics.stage_statistics"]);
    println!("cargo:rerun-if-changed=protos");

    let files: Vec<PathBuf> = visit_files(Path::new("protos")).collect();

    let result = builder
        .include_file("includes.rs")
        .bytes(["."])
        .emit_rerun_if_changed(false)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos_with_config(config, &files, includes);

    if let Err(err) = result {
        eprintln!("{err:#}");
        std::process::exit(1);
    }

    Ok(())
}
