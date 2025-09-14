fn main() {
    tonic_prost_build::compile_protos("proto/storage.proto").unwrap();
    tonic_prost_build::compile_protos("proto/error_details.proto").unwrap();
}