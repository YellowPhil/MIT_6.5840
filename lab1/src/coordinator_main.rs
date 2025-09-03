use mapreduce::coordinator::coordinator::Coordinator;

pub mod fileinfo {
    include!(concat!(env!("OUT_DIR"), "/fileinfo.rs"));
}

fn main() {
    let coordinator = Coordinator::new();
}