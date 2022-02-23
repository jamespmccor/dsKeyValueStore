package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.framework.*;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.text.SimpleDateFormat;
import java.util.*;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {

  public enum ServerState {
    FOLLOWER, LEADER, ELECTING_LEADER
  }

  public static boolean PRINT_DEBUG = DebugUtils.PaxosServer_DEBUG;

  public static final int REPLICA_LEADER_WAIT = 1;
  public static final int REPLICA_ELECTING_LEADER_WAIT = 1;
  public static final int REPLICA_FOLLOWER_WAIT = 5;
  public static final int INITIAL_BALLOT_NUMBER = -1;

  /**
   * All servers in the Paxos group, including this one.
   */
  private final Address[] servers;

  private final AMOApplication<Application> app;
  private final VoteTracker voteTracker;
  private final PaxosLog log;

  private ServerState serverState;
  private Set<Address> votes;
  private Ballot leaderBallot;
  private int tick = 0;

  /* -------------------------------------------------------------------------
      Construction and Initialization
     -----------------------------------------------------------------------*/
  public PaxosServer(Address address, Address[] servers, Application app) {
    super(address);
    this.servers = servers;

    this.app = new AMOApplication<>(app);
    log = new PaxosLog();
    voteTracker = new VoteTracker(servers, log);

    serverState = ServerState.ELECTING_LEADER;
    votes = new HashSet<>();
    leaderBallot = new Ballot(INITIAL_BALLOT_NUMBER, servers[0]);
  }

  @Override
  public void init() {
    startLeaderElection();
    set(new HeartBeatTimer(), HeartBeatTimer.SERVER_TICK_MILLIS);
  }

    /* -------------------------------------------------------------------------
        Interface Methods

        Be sure to implement the following methods correctly. The test code uses
        them to check correctness more efficiently.
       -----------------------------------------------------------------------*/

  /**
   * Return the status of a given slot in the server's local log.
   * <p>
   * If this server has garbage-collected this slot, it should return {@link PaxosLogSlotStatus#CLEARED} even if it has
   * previously accepted or chosen command for this slot. If this server has both accepted and chosen a command for this
   * slot, it should return {@link PaxosLogSlotStatus#CHOSEN}.
   * <p>
   * Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's status
   * @see PaxosLogSlotStatus
   */
  public PaxosLogSlotStatus status(int logSlotNum) {
    return log.getLogStatus(logSlotNum);
  }

  /**
   * Return the command associated with a given slot in the server's local log.
   * <p>
   * If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link PaxosLogSlotStatus#EMPTY}, this method should
   * return {@code null}. Otherwise, return the command this server has chosen or accepted, according to {@link
   * PaxosServer#status}.
   * <p>
   * If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this method should unwrap them before
   * returning.
   * <p>
   * Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's contents or {@code null}
   * @see PaxosLogSlotStatus
   */
  public Command command(int logSlotNum) {
    return log.contains(logSlotNum) && log.getLog(logSlotNum).amoCommand() != null ? log.getLog(logSlotNum).amoCommand().command() : null;
  }

  /**
   * Return the index of the first non-cleared slot in the server's local log. The first non-cleared slot is the first
   * slot which has not yet been garbage-collected. By default, the first non-cleared slot is 1.
   * <p>
   * Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int firstNonCleared() {
    return log.getFirstNonCleared();
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, according to the defined states in {@link
   * PaxosLogSlotStatus}. If there are no non-empty slots in the log, this method should return 0.
   * <p>
   * Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int lastNonEmpty() {
    return log.getLastNonEmpty();
  }

  /* -------------------------------------------------------------------------
      Message Handlers
     -----------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    if (!isLeader()) {
//      debugMsg("ignored msg server state", serverState.toString(), m.toString());
      return;
    }

    debugSenderMsg(sender, "ack paxos req num", Integer.toString(m.cmd().num()), m.toString());
    if (app.alreadyExecuted(m.cmd())) {
      if (app.execute(m.cmd()) != null) {
        send(new PaxosReply(app.execute(m.cmd())), sender);
      }
    } else if (log.indexOfCommand(m.cmd()) > 0) {
      send2A(log.getLog(log.indexOfCommand(m.cmd())));
    } else {
      LogEntry logEntry = voteTracker.createLogEntry(getBallot(), m.cmd());
      voteTracker.addLogEntry(logEntry);
      send2A(logEntry);
    }
  }

  // leader election
  private void handlePaxos1A(Paxos1A m, Address sender) {
    debugSenderMsg(sender, "recv 1a, ballot", m.ballot().toString());

    if (m.ballot().compareTo(leaderBallot) > 0) {
      //might have to save ballot, but I think it's fine
      debugSenderMsg(sender, "acc 1a -> follower, ballot", m.ballot().toString());
      setLeader(m.ballot());
      accept1B(leaderBallot);
    } else if (m.ballot().compareTo(leaderBallot) == 0) {
      // should only happen on the leader.
      accept1B(leaderBallot);
    } else {
      debugSenderMsg(sender, "reject 1a, ballot", m.ballot().toString(), "sending", leaderBallot.toString());
      reject1B(sender, leaderBallot);
    }
  }

  private void handlePaxos1B(Paxos1B m, Address sender) {
    debugSenderMsg(sender, "recv ballot", m.toString(), "votes", votes.toString());
    if (m.accepted()) {
      if (m.ballot().compareTo(leaderBallot) > 0) {
        assert false;
      } else if (m.ballot().compareTo(leaderBallot) == 0) {
        if (isElectingLeader()) {
          debugSenderMsg(sender, "acc 1b, ballot", m.ballot().toString(), "votes", votes.toString());
          log.fastForwardLog(m.log());
          if (voteLeaderElection(sender, m.ballot())) {
            setLeader(leaderBallot);
            log.fillNoOps(leaderBallot);
            executeLog();
            rebroadcastAcceptedLogEntries(log);
            sendHeartBeat();
          }
          debugSenderMsg(sender, "recv ballot post", m.toString(), "votes", votes.toString());
        }
      } else {
        debugSenderMsg(sender, "reject 1b, ballot", m.ballot().toString(), "sending", leaderBallot.toString());
        if (!this.address().equals(sender)) {
          reject1B(sender, leaderBallot);
        }
      }
    } else {
      if (m.ballot().compareTo(leaderBallot) > 0) {
        //might have to save ballot, but I think it's fine
        debugSenderMsg(sender, "acc 1b -> follower, ballot", m.ballot().toString());
        setLeader(m.ballot());
        accept1B(leaderBallot);
      } else {
        debugSenderMsg(sender, "reject 1b, ballot", m.ballot().toString());
      }
    }
  }

  /**
   * Takes an accepted ballot, and then adds it as a {@link PaxosLogSlotStatus#ACCEPTED} slot. Send out message telling
   * all replicas accepted/rejected the message.
   * <p>
   * P2A(slot, seq, logentry) => if (leader) => set(seq, logentry) for slot send2B(slot, seq)
   *
   * @param m
   * @param sender
   */
  private void handlePaxos2A(Paxos2A m, Address sender) {
    if (isElectingLeader() || !m.leaderBallot().equals(leaderBallot)) {
      return;
    }
    debugSenderMsg(sender, "recv 2a slot", Integer.toString(m.entry().slot()));
    if (!isLeader(sender)) {
//      debugMsg("reject sender is not leader", Integer.toString(m.entry().slot()));
      return;
    }

    if (isLeader()) {
//      debugMsg("leader self-voted 2a slot", Integer.toString(m.entry().slot()));
      voteTracker.vote(address(), m.entry());
      send2B(m.entry());
      return;
    }

    LogEntry existingLog = log.getLog(m.entry().slot());
    if (existingLog == null || (m.entry().status().compareTo(existingLog.status()) > 0
        || m.entry().ballot().compareTo(existingLog.ballot()) > 0)) {
//      debugSenderMsg(sender, "updated log 2a slot", Integer.toString(m.entry().slot()));

      // weird case of already exists in different slot on different server
      log.updateLog(m.entry().slot(), m.entry());
    }

//    debugSenderMsg(sender, "voted 2a slot", Integer.toString(m.entry().slot()));
    send2B(m.entry());
  }

  /**
   * Receive the accept/reject from the replicas. P2B(slot, seq) => if (leader) => if (slot, seq) is equal => set(slot,
   * seq) to confirmed
   *
   * @param m
   * @param sender
   */
  private void handlePaxos2B(Paxos2B m, Address sender) {
    if (isElectingLeader()) {
      return;
    }
//    debugSenderMsg(sender, "ack 2b", "for entry", m.entry().toString());
    if (!isLeader()) {
//      debugSenderMsg(sender, "ignored b/c not leader");
      return;
    }

    if (!voteTracker.vote(sender, m.entry())) {
//      debugMsg("ignored vote", m.entry().toString());
    }
    executeLog();
//    debugMsg("2b execution state: ", log.getLog(log.indexOfCommand(m.entry().amoCommand())).toString());
  }

  private void handleHeartBeat(HeartBeat tick, Address sender) {
    if (sender.equals(this.address()) || tick.leaderBallot().compareTo(leaderBallot) < 0) {
      return;
    }
    if (tick.leaderBallot().compareTo(leaderBallot) > 0) {
      setLeader(tick.leaderBallot());
    }
    if (tick.leaderBallot().compareTo(leaderBallot) == 0) {
      resetTimers();
      debugSenderMsg(sender, "heartbeat ack", tick.leaderBallot().toString());
      log.fastForwardLog(tick.log());
      executeLog();
      rebroadcastAcceptedLogEntries(tick.log());

    }
  }

  /* -------------------------------------------------------------------------
      Timer Handlers
     -----------------------------------------------------------------------*/

  private void onHeartBeatTimer(HeartBeatTimer ht) {
    tick--;
    if (tick > 0) {
      set(ht, HeartBeatTimer.SERVER_TICK_MILLIS);
      return;
    } else if (isLeader()) {
      fireLeader();
    } else if (isFollower()) {
      fireFollower();
    } else if (isElectingLeader()) {
      fireElectingLeader();
    }
    resetTimers();
    set(ht, HeartBeatTimer.SERVER_TICK_MILLIS);
  }

  private void fireLeader() {
    sendHeartBeat();
  }

  private void fireFollower() {
    setServerState(ServerState.ELECTING_LEADER);
    startLeaderElection();
  }

  private void fireElectingLeader() {
    send1A(leaderBallot);
  }

  private void resetTimers() {
    debugMsg("reset timers for ", serverState.toString());
    switch (serverState) {
      case LEADER:
        tick = REPLICA_LEADER_WAIT;
        break;
      case ELECTING_LEADER:
        tick = REPLICA_ELECTING_LEADER_WAIT;
        break;
      case FOLLOWER:
      default:
        tick = REPLICA_FOLLOWER_WAIT;
    }
  }

    /* -------------------------------------------------------------------------
        Log Utils
       -----------------------------------------------------------------------*/

  private void executeLog() {
//    debugMsg("executing log");
    LogEntry cur = log.getAndIncrementFirstUnexecuted();
    while (cur != null) {
      if (cur.amoCommand() != null) {
//        debugMsg("\texecuting log for slot", Integer.toString(cur.slot()));
        PaxosReply reply = new PaxosReply(app.execute(cur.amoCommand()));
        if (isLeader()) {
//          debugMsg("\tsending res for slot", Integer.toString(cur.slot()));
          send(reply, cur.amoCommand().sender());
        }
      }
      cur = log.getAndIncrementFirstUnexecuted();
    }
  }

  /* -------------------------------------------------------------------------
      Send Utils
     -----------------------------------------------------------------------*/

  private void send1A(Ballot ballot) {
    debugMsg("send 1a for election in slot: " + ballot.seqNum());
    Paxos1A proposal = new Paxos1A(ballot);
    serverBroadcast(proposal);
  }

  private void accept1B(Ballot b) {
    send1B(b.leader(), true, b);
  }

  private void reject1B(Address sender, Ballot b) {
//    send1B(sender, false, b);
  }

  private void send1B(Address sender, boolean accept, Ballot ballot) {
    debugMsg("sending 1b(" + accept + ") to", sender.toString(), "ballot", ballot.toString());
    Paxos1B response = new Paxos1B(accept, ballot, log);
    sendServer(response, sender);
  }

  private void send2A(LogEntry e) {
//    debugMsg("send 2a, slot:", Integer.toString(e.slot()), e.toString());
    Paxos2A proposal = new Paxos2A(e, leaderBallot);
    serverBroadcast(proposal);
  }

  private void send2B(LogEntry logEntry) {
//    debugMsg("send 2b, slot:", Integer.toString(logEntry.slot()), logEntry.toString());
    sendServer(new Paxos2B(new LogEntry(logEntry, getBallot())), leaderBallot.leader());
  }

  private void serverBroadcast(Message m) {
    for (Address a : servers) {
      if (!a.equals(this.address())) {
        send(m, a);
      }
    }
    sendServer(m, this.address());
  }

  /**
   * IF THERE ARE MULTIPLE CHOSEN VALUES FOR A SINGLE SLOT THIS METHOD WILL BROADCAST INCORRECT VALUES
   *
   * @param l
   */
  private void rebroadcastAcceptedLogEntries(PaxosLog l) {
    for (LogEntry e : l.log().values()) {
      if (e.status() == PaxosLogSlotStatus.ACCEPTED && log.getLog(e.slot()) != null) {
        send2B(log.getLog(e.slot()));
      }
    }
  }

  private void sendServer(Message m, Address dest) {
    if (this.address().equals(dest)) {
      this.handleMessage(m);
    } else {
      send(m, dest);
    }
  }


  /* -------------------------------------------------------------------------
      Server State Utils
     -----------------------------------------------------------------------*/

  private Ballot startLeaderElection() {
    //TODO: REMOVE INVARIANT CHECK
    assert isElectingLeader();

    Ballot ballot = new Ballot(leaderBallot.seqNum() + 1, this.address());
    debugMsg("ServerState:", serverState.toString(), "Starting new leader election seqNum " + getSeqNum(), "->",
        "" + ballot.seqNum());

    setElectingLeader(ballot);
    send1A(ballot);
    return ballot;
  }

  private void setLeader(Ballot b) {
    if (b.equals(leaderBallot)) {
      if (isLeader()) {
        return;
      } else if (isElectingLeader()) {
        setServerState(ServerState.LEADER);
      } else {
        throw new Error("invalid state");
      }
    } else {
      setServerState(ServerState.FOLLOWER);
      debugMsg("clearing votes", votes.toString());
      votes.clear();
    }
    leaderBallot = b;
  }

  private void setElectingLeader(Ballot b) {
    assert b.leader().equals(address());
    setServerState(ServerState.ELECTING_LEADER);
    debugMsg("clearing votes", votes.toString());
    votes.clear();
    leaderBallot = b;
  }

  // returns true if vote succeeded
  private boolean voteLeaderElection(Address sender, Ballot b) {
    debugMsg("election pre", b.toString(), leaderBallot.toString(), "(self leader):", votes.toString());

    assert b.equals(leaderBallot);
    assert leaderBallot.leader().equals(this.address());

    if (isLeader() && b.equals(leaderBallot)) {
      debugMsg("election already succeeded");
      return true;
    }

    votes.add(sender);
    debugMsg("election", leaderBallot.toString(), "(self leader):", votes.toString());

    if (votes.size() > servers.length / 2) {
      return true;
    }
    return false;
  }

  private boolean isLeader() {
    return serverState == ServerState.LEADER;
  }

  private boolean isFollower() {
    return serverState == ServerState.FOLLOWER;
  }

  private boolean isElectingLeader() {
    return serverState == ServerState.ELECTING_LEADER;
  }

  private boolean isLeader(Address sender) {
    return sender.equals(leaderBallot.leader());
  }

  private void sendHeartBeat() {
    assert isLeader() && leaderBallot.leader().equals(this.address());

    debugMsg("sending heartbeat");
    serverBroadcast(new HeartBeat(getBallot(), log));
  }

  private void setServerState(ServerState state) {
    assert state != ServerState.LEADER || serverState != ServerState.FOLLOWER;
    boolean changeServerState = state != serverState;
    debugMsg("Server state", serverState.toString(), "->", state.toString());
    serverState = state;
    if (changeServerState) {
      resetTimers();
    }
  }

  private int getSeqNum() {
    return leaderBallot.seqNum();
  }

  private Ballot getBallot() {
    return leaderBallot;
  }

    /* -------------------------------------------------------------------------
    Debug
    -----------------------------------------------------------------------*/

  private void debugSenderMsg(Address sender, String... msgs) {
    debugMsg("<-", sender.toString(), String.join(" ", msgs));
  }

  private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

  private void debugMsg(String... msgs) {
    if (PRINT_DEBUG) {
      System.out.println(sdf2.format(new Date()) + " " + this.address().toString() + ": " + String.join(" ", msgs));
    }
  }

}
