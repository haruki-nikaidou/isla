fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/preauth.proto")?;
    tonic_prost_build::compile_protos("proto/account.proto")?;
    Ok(())
}
