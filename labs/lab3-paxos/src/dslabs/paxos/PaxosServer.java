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
    FOLLOWER, LEADER, ELECTING_LEADER;
  }

  public static final boolean PRINT_DEBUG = true;
  private static final int LEADER_TICK_COUNT = 2;


  /**
   * All servers in the Paxos group, including this one.
   */
  private final Address[] servers;
  private final int serverNum;

  private final AMOApplication<Application> app;
  private final VoteTracker voteTracker;
  private final PaxosLog log;

  private ServerState serverState;
  private Ballot leaderBallot;
  private int roundNum;
  private int leaderTick;
  private final Set<Address> leaderVotes;

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
    serverNum = Arrays.binarySearch(servers, this.address());
    roundNum = 0;
    leaderVotes = new HashSet<>();
    leaderBallot = getBallot();
  }


  @Override
  public void init() {
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
    return log.contains(logSlotNum) ? log.getLog(logSlotNum).amoCommand().command() : null;
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
    if (isLeader()) {
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
  }

  private void handlePaxos1A(Paxos1A m, Address sender){
    if(serverState == ServerState.ELECTING_LEADER
      && m.ballot().compareTo(leaderBallot) > 0){
        updateLeader(m.ballot());
        send1B();
    }
  }

  private void handlePaxos1B(Paxos1B m, Address sender){
    if(serverState == ServerState.ELECTING_LEADER
      && m.ballot().equals(leaderBallot)){
        debugSenderMsg(sender, "ack 1b round", Integer.toString(roundNum));
        leaderVotes.add(sender);
        log.fastForwardLog(m.log());
        if(leaderVotes.size() > servers.length / 2){
          log.fillHoles(leaderBallot);
          setServerState(ServerState.LEADER);
          sendHeartBeat();
        }
    }
  }
//    // leader election
//    private void handlePaxos1A(Paxos1A m, Address sender) {
//        if (!checkLeaderAlive() && m.ballot().compareTo(leaderBallot) > 0) {
//            debugSenderMsg(sender, "ack 1a, ballot", m.ballot().toString());
//            //might have to save ballot, but I think it's fine
//            leaderBallot = m.ballot();
//            send1B();
//        }
//    }
//
//    private void handlePaxos1B(Paxos1B m, Address sender) {
//        if (!isLeader() && myBallot.equals(m.ballot())) {
//            debugSenderMsg(sender, "ack 1b", m.ballot().toString());
//            if (ack1B(m, sender)) {
//                //repropose with no-ops
//                fillLogNoOps();
//                resetProposals();
//                sendHeartBeat();
//            }
//        }
//    }


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
    debugSenderMsg(sender, "recv 2a slot", Integer.toString(m.entry().slot()));
    if (isLeader()) {
      debugMsg("leader self-voted 2a slot", Integer.toString(m.entry().slot()));
      voteTracker.vote(m.entry());
      send2B(m.entry());
      return;
    }

    if (!isLeader(sender)) {
      debugMsg("reject sender is not leader", Integer.toString(m.entry().slot()));
      return;
    }

    LogEntry existingLog = log.getLog(m.entry().slot());
    if (existingLog == null || (m.entry().status().compareTo(existingLog.status()) > 0
        || m.entry().ballot().roundNum() > existingLog.ballot().roundNum())) {
      debugSenderMsg(sender, "updated log 2a slot", Integer.toString(m.entry().slot()));
      log.updateLog(m.entry().slot(), m.entry());
    }

    debugSenderMsg(sender, "voted 2a slot", Integer.toString(m.entry().slot()));
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
    debugSenderMsg(sender, "ack 2b", "for entry", m.entry().toString());
    if (!isLeader()) {
      debugSenderMsg(sender, "ignored b/c not leader");
      return;
    }

    if (!voteTracker.vote(m.entry())) {
      debugMsg("ignored vote", m.entry().toString());
    }
    executeLog();
  }

  private void handleHeartBeat(HeartBeat tick, Address sender) {
    if (!isLeader() && tick.leaderBallot().compareTo(leaderBallot) >= 0) {
      debugSenderMsg(sender, "heartbeat ack", tick.leaderBallot().toString());
      setServerState(ServerState.FOLLOWER);
      updateLeader(tick.leaderBallot());
      log.fastForwardLog(tick.log());
      executeLog();

      // rebroadcast accepted values so leader can add them to the vote.
      for (LogEntry e : tick.log().getLog().values()) {
        if (e.status() == PaxosLogSlotStatus.ACCEPTED) {
          send2B(e);
        }
      }
    }
  }


  /* -------------------------------------------------------------------------
      Timer Handlers
     -----------------------------------------------------------------------*/

  private void onHeartBeatTimer(HeartBeatTimer ht) {
    if (isLeader()) {
      sendHeartBeat();
      set(ht, HeartBeatTimer.SERVER_TICK_MILLIS);
    } else if(!tickLeader()){
      setServerState(ServerState.ELECTING_LEADER);
      roundNum++;
      if(roundNum % servers.length == serverNum){
        send1A();
        set(ht, HeartBeatTimer.SERVER_TICK_MILLIS);
      } else{
        set(ht, HeartBeatTimer.ELECTION_TICK_MILLIS);
      }
    }else{
      set(ht, HeartBeatTimer.SERVER_TICK_MILLIS);
    }

  }

    /* -------------------------------------------------------------------------
        Log Utils
       -----------------------------------------------------------------------*/

  private void executeLog() {
    debugMsg("executing log");
    LogEntry cur = log.getAndIncrementFirstUnexecuted();
    while (cur != null) {
      if (cur.amoCommand() != null) {
        debugMsg("\texecuting log for slot", Integer.toString(cur.slot()));
        PaxosReply reply = new PaxosReply(app.execute(cur.amoCommand()));
        if (isLeader()) {
          debugMsg("\tsending res for slot", Integer.toString(cur.slot()));
          send(reply, cur.amoCommand().sender());
        }
      }
      cur = log.getAndIncrementFirstUnexecuted();
    }
  }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/

  private void send1A(){
    debugMsg("send 1a, round", Integer.toString(roundNum));
    serverBroadcast(new Paxos1A(getBallot(), log));
  }

  private void send1B(){
    debugMsg("send 1b to", leaderBallot.toString());
    sendServer(new Paxos1B(leaderBallot, log), leaderBallot.sender());
  }

  private void send2A(LogEntry e) {
    debugMsg("send 2a, slot:", Integer.toString(e.slot()), e.toString());
    Paxos2A proposal = new Paxos2A(e, leaderBallot);
    serverBroadcast(proposal);
  }

  private void send2B(LogEntry logEntry) {
    sendServer(new Paxos2B(new LogEntry(logEntry, getBallot())), leaderBallot.sender());
  }

//
//    private boolean ack1B(Paxos1B b, Address sender) {
//        proposals.get(LEADER_ELECTION_SLOT).received2B().add(sender);
//        catchUpLog(b.log());
//        debugMsg(Integer.toString(proposals.get(LEADER_ELECTION_SLOT).received2B().size()), "/", Integer.toString(servers.length), "ballot", myBallot.toString());
//        if (proposals.get(LEADER_ELECTION_SLOT).received2B().size() > servers.length / 2) {
//            debugMsg("IS LEADER");
//            myBallot.seqNum(myBallot.seqNum() + 1);
//            updateLeader(myBallot);
//        }
//        return isLeader();
//    }
//
//    /**
//     * @param b
//     * @param sender
//     * @return true if > 1/2 servers respond
//     */
//    private boolean ack2B(Paxos2B b, Address sender) {
//        proposals.get(b.slot()).received2B().add(sender);
//        return proposals.get(b.slot()).received2B().size() > servers.length / 2;
//    }

  private boolean isLeader() {
    return serverState == ServerState.LEADER;
  }

  private boolean isFollower() {
    return serverState == ServerState.FOLLOWER;
  }

  private boolean isLeader(Address sender) {
    return sender.equals(leaderBallot.sender());
  }

  private boolean checkLeaderAlive() {
    return serverState != ServerState.ELECTING_LEADER;
  }

  private boolean tickLeader(){
    leaderTick--;
    return leaderTick > 0;
  }

  private void sendHeartBeat() {
    debugMsg("sending heartbeat");
    serverBroadcast(new HeartBeat(getBallot(), log));
  }

  private void updateLeader(Ballot newLeader) {
    leaderTick = LEADER_TICK_COUNT;
    leaderBallot = newLeader;
    roundNum = newLeader.roundNum();
  }

  private void serverBroadcast(Message m) {
    for (Address a : servers) {
      if (!a.equals(this.address())) {
        send(m, a);
      }
    }
    sendServer(m, this.address());
  }

  private void sendServer(Message m, Address dest) {
    if (this.address().equals(dest)) {
      this.handleMessage(m);
    } else {
      send(m, dest);
    }
  }

  private void setServerState(ServerState state) {
    debugMsg("Server state set to", state.toString());
    serverState = state;
  }

  private Ballot getBallot() {
    return new Ballot(roundNum, this.address());
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
