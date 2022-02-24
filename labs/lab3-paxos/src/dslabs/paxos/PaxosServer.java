package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.framework.*;
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

  public static final int REPLICA_LEADER_WAIT = 1;
  public static final int REPLICA_ELECTING_LEADER_WAIT = 1;
  public static final int REPLICA_FOLLOWER_WAIT = 4;
  public static final int INITIAL_BALLOT_NUMBER = -1;

  /**
   * All servers in the Paxos group, including this one.
   */
  private final Address[] servers;

  private final AMOApplication<Application> app;
  private final VoteTracker voteTracker;
  private final PaxosLog log;

  private ServerState serverState;
  private HashMap<Address, Integer> minUnexecutedVals;
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

    minUnexecutedVals = new HashMap<>();
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
    return log.contains(logSlotNum) && log.getLog(logSlotNum).amoCommand() != null ? log.getLog(logSlotNum).amoCommand()
        .command() : null;
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
      return;
    }

    if (app.alreadyExecuted(m.cmd())) {
      if (app.execute(m.cmd()) != null) {
        send(new PaxosReply(app.execute(m.cmd())), sender);
      }
    } else if (log.commandExistsInLog(m.cmd())) {
      send2A(log.getLog(log.indexesOfCommand(m.cmd()).iterator().next()));
    } else {
      LogEntry logEntry = voteTracker.createLogEntry(getBallot(), m.cmd());
      voteTracker.addLogEntry(logEntry);
      send2A(logEntry);
    }
  }

  // leader election
  private void handlePaxos1A(Paxos1A m, Address sender) {
    if (m.ballot().compareTo(leaderBallot) > 0) {
      //might have to save ballot, but I think it's fine
      setLeader(m.ballot());
      accept1B(leaderBallot);
    } else if (m.ballot().compareTo(leaderBallot) == 0) {
      // should only happen on the leader.
      accept1B(leaderBallot);
    }
  }

  private void handlePaxos1B(Paxos1B m, Address sender) {
    if (m.ballot().compareTo(leaderBallot) > 0) {
      setLeader(m.ballot());
      accept1B(leaderBallot);
    } else if (m.ballot().compareTo(leaderBallot) == 0) {
      if (isElectingLeader()) {
        log.fastForwardLog(m.log());
        if (voteLeaderElection(sender, m.ballot())) {
          setLeader(leaderBallot);
          log.fillNoOps(leaderBallot);
          executeLog();
          rebroadcastAcceptedLogEntries(log);
          sendHeartBeat();
        }
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
    if (!isLeader(sender) || log.getLogStatus(m.entry().slot()) == PaxosLogSlotStatus.CLEARED) {
      return;
    }

    if (isLeader()) {
      voteTracker.vote(address(), m.entry());
      send2B(m.entry());
      return;
    }

    LogEntry existingLog = log.getLog(m.entry().slot());
    if (existingLog == null || (m.entry().status().compareTo(existingLog.status()) > 0
        || m.entry().ballot().compareTo(existingLog.ballot()) > 0)) {

      // weird case of already exists in different slot on different server
      log.updateLog(m.entry().slot(), m.entry());
    }

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
    if (!isLeader() || log.getLogStatus(m.entry().slot()) == PaxosLogSlotStatus.CLEARED) {
      return;
    }

    voteTracker.vote(sender, m.entry());
    executeLog();
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
      log.fastForwardLog(tick.log());
      executeLog();
      rebroadcastAcceptedLogEntries(tick.log());

      if (log.min_slot() < tick.log().min_slot()) {
        log.garbageCollect(tick.log().min_slot());
      }

      sendHeartbeatResponse();
    }
  }

  private void handleHeartBeatResponse(HeartBeatResponse tick, Address sender) {
    if (isFollower()) {
      return;
    }
    minUnexecutedVals.put(sender,
        Math.max(minUnexecutedVals.getOrDefault(sender, tick.garbageSlot()), tick.garbageSlot()));
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
    LogEntry cur = log.getAndIncrementFirstUnexecuted();
    while (cur != null) {
      if (cur.amoCommand() != null) {
        PaxosReply reply = new PaxosReply(app.execute(cur.amoCommand()));
        if (isLeader()) {
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
    Paxos1A proposal = new Paxos1A(ballot);
    serverBroadcast(proposal);
  }

  private void accept1B(Ballot b) {
    send1B(b.leader(), b);
  }

  private void send1B(Address sender, Ballot ballot) {
    Paxos1B response = new Paxos1B(ballot, log);
    sendServer(response, sender);
  }

  private void send2A(LogEntry e) {
    Paxos2A proposal = new Paxos2A(e, leaderBallot);
    serverBroadcast(proposal);
  }

  private void send2B(LogEntry logEntry) {
    sendServer(new Paxos2B(new LogEntry(logEntry, getBallot())), leaderBallot.leader());
  }


  private void sendHeartBeat() {
    if (minUnexecutedVals.size() == servers.length - 1 && servers.length > 1) {
      int globalMin = minUnexecutedVals.values().stream().reduce(Math::min).orElseThrow();
      globalMin = Math.min(globalMin, log.min_slot_unexecuted());
      if (log.min_slot() < globalMin) {
        log.garbageCollect(globalMin - 1);
      }
    }
    minUnexecutedVals.clear();

    serverBroadcast(new HeartBeat(getBallot(), log));
  }

  private void sendHeartbeatResponse() {
    sendServer(new HeartBeatResponse(log.min_slot_unexecuted()), leaderBallot.leader());
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
    Ballot ballot = new Ballot(leaderBallot.seqNum() + 1, this.address());

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
      votes.clear();
    }
    leaderBallot = b;
  }

  private void setElectingLeader(Ballot b) {
    assert b.leader().equals(address());
    setServerState(ServerState.ELECTING_LEADER);
    votes.clear();
    leaderBallot = b;
  }

  // returns true if vote succeeded
  private boolean voteLeaderElection(Address sender, Ballot b) {
    if (isLeader() && b.equals(leaderBallot)) {
      return true;
    }

    votes.add(sender);

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

  private void setServerState(ServerState state) {
    assert state != ServerState.LEADER || serverState != ServerState.FOLLOWER;
    boolean changeServerState = state != serverState;
    serverState = state;
    if (changeServerState) {
      resetTimers();
    }
    if (isFollower()) {
      minUnexecutedVals.clear();
    }
  }

  private int getSeqNum() {
    return leaderBallot.seqNum();
  }

  private Ballot getBallot() {
    return leaderBallot;
  }
}
