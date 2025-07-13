fn main () -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../proto/hermes.proto")?;
    tonic_build::compile_protos("replication.proto")?;
    Ok(())
}
