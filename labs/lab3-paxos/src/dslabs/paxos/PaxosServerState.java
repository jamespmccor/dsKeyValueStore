//package dslabs.paxos;
//
//import dslabs.framework.Address;
//import dslabs.paxos.PaxosServer.ServerState;
//import java.util.HashSet;
//import java.util.Set;
//
///**
// * Basically acts as a vote tracker / log for the server election. Scope of this is to handle 3 core functions. - new
// * generation: starts a new vote slot - takes up a new slot
// * <p>
// * - 1a messages send a ballot for the current server generation to every server - 1b messages are a reply of either
// * an identical ballot back, or the server's leader.
// * <p>
// * - heartbeat causes new generation if no leader elected - if a voteGranted is returned from 1a with same message
// * then it should consider as a vote for election - if a voteRejection(ballot) is returned then there are 2 cases: -
// * ballot is smaller - not possible? - ballot is larger - already enforcing leader - if already in ELECTING_LEADER
// * state don't let it increment leader generation unless it's on a non-aligned seqNum? - on consensus force a leader
// * doc Inner class so it can directly send messages and set outer variables.
// *
// * model:
// *
// *  1a   1b
// *   / a \
// * a - b - a
// *   \ c /
// */
//public class PaxosServerState {
//
//  private final int servers;
//
//  private ServerState state;
//
//  private int seqOffset;
//  private int leaderSeqNum;
//  private Set<Address> leaderElection;
//
//  public PaxosServerState(int servers, int seqOffset) {
//    leaderElection = new HashSet<>();
//    this.seqOffset = seqOffset;
//    this.servers = servers;
//
//    state = ServerState.ELECTING_LEADER;
//  }
//
//  public Ballot startLeaderElection() {
//    //TODO: REMOVE INVARIANT CHECK
//    assert state == ServerState.ELECTING_LEADER;
//
//    System.out.println("starting new leader election seqNum " + getAlignedSeqNum());
//
//    Ballot ballot = new Ballot(getAlignedSeqNum(), PaxosServer.this.address());
//    return ballot;
//  }
//
//  /**
//   *
//   * If dropped -> affects leaderElection
//   * If delayed -> seqNum < since each server reserves an offset for a round
//   * If success -> should add to leader election
//   * @param m
//   * @return true if vote succeeded an ELECTING_LEADER -> LEADER/FOLLOWER
//   */
//  public boolean acceptVote(Paxos1A m) {
//    // Ignore < seqNum ballots since by definition must be from same round.
//    if (m.ballot().seqNum() < seqNum) {
//      send1B(false, getBallot());
//    } else if (m.ballot().seqNum() == seqNum) {
//
//    }
//  }
//
//  public ServerState getServerState() {
//    return state;
//  }
//
//  private int getSeqNum() {
//
//  }
//
//  private Ballot getBallot() {
//    return new Ballot(getSeqNum(), l)
//  }
//
//  private int getAlignedSeqNum() {
//    return 1 + seqOffset + seqRound * servers;
//  }
//
//  private int round(int seqNum) {
//    return (seqNum - 1) / servers;
//  }
//}