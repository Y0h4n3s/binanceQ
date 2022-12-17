fn main() -> Result<(), Box<dyn std::error::Error>> {
	std::process::Command::new("python3")
		  .arg("-m grpc_tools.protoc")
		  .arg("-I managers/proto")
		  .arg("--python_out=./python/signals/")
		  .arg("--pyi_out=./python/signals/")
		  .arg("--grpc_python_out=./python/signals/")
		  .arg("managers/proto/signals.proto")
		  .spawn()?;
	Ok(())
}