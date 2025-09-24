use std::time::Duration;

enum State {
    Follower,
    Candidate,
    Leader,
}
pub struct ConensusClient {
    state: State,
    others: Vec<String>,
    current_term: u64,
    term_timer: tokio::time::Interval,
    vote_timer: tokio::time::Interval,
}

impl ConensusClient {
    pub fn new(others: Vec<String>, term_duration: Duration, vote_duration: Duration) -> Self {
        Self {
            state: State::Follower,
            others,
            current_term: 0,
            term_timer: tokio::time::interval(term_duration),
            vote_timer: tokio::time::interval(vote_duration),
        }
    }
}

impl ConensusClient {
    pub fn start_election(&mut self) {
        self.current_term += 1;
        self.state = State::Candidate;
    }
}
