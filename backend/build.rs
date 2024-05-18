use tonic_build;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("protos/model_config.proto")?;
    tonic_build::compile_protos("protos/triton.proto")?;
    Ok(())
}
