fn main() {
    prost_build::compile_protos(&["proto/fileinfo.proto"], &["proto"]).unwrap();
}