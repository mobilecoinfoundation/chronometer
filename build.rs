use flatc_rust;

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=data/unsequenced.fbs");
    let flatc = flatc_rust::Flatc::from_env_path();
    let version = flatc.version().expect("couldn't get flatc version");
    println!("{}", version.version());
    assert!(version.version().starts_with("2"));
    flatc.run(flatc_rust::Args {
        inputs: &[Path::new("data/unsequenced.fbs")],
        out_dir: Path::new("target/flatbuffers/"),
        ..Default::default()
    }).expect("flatc");
}
