// Compiles the tracker.v1 proto this module owns.
//
// One file, one package. If a second surface is ever added (an event/webhook
// contract, say), it belongs HERE alongside — the forge module's events.proto
// and discovery.proto were vendored byte-identical into three other repos
// precisely because they had no home, and a copy is faithful only until someone
// edits one.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/tracker/v1/tracker.proto"], &["proto"])?;
    Ok(())
}
