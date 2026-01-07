
/// State of a Raft process with a corresponding (volatile) information.
#[derive(Default)]
enum ProcessType {
    #[default]
    Follower,
    Candidate {
        votes_received: HashSet<Uuid>,
    },
    Leader,
}