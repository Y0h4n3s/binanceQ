use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	tonic_build::compile_protos("proto/signals.proto")?;
	
	Command::new("python3")
		  .arg("-m grpc_tools.protoc")
		  .arg("-Iproto")
		  .arg("--python_out=../python/signals/")
		  .arg("--pyi_out=../python/signals/")
		  .arg("--grpc_python_out=../python/signals/")
		  .arg("proto/signals.proto")
		  .spawn()?;
	Ok(())
}