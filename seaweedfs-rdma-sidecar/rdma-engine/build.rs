fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&["proto/volume_server_min.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/volume_server_min.proto");
    Ok(())
}
