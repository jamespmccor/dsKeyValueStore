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

    public static boolean PRINT_DEBUG = true;

    public static final int LOG_INITIAL = 1;

    public static final int REPLICA_LEADER_WAIT = 3;

    public static final int LEADER_ELECTION_SLOT = 0;

    public static final int GARBAGE_SLOT = -1;
        
    /**
     * All servers in the Paxos group, including this one.
     */
    private final Address[] servers;

    private final Map<Integer, LogEntry> log;
    private final Ballot ballot;
    private final AMOApplication<Application> app;
    private final Map<Integer, ProposedSlot> proposals; // leader specific

    private Address leader;
    private int leader_tick_miss;

    private int slot_in; // leader puts new proposals here
    private int slot_out; // first unexecuted slot


    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        this.app = new AMOApplication<>(app);
        ballot = new Ballot(0, this.address());
        log = new HashMap<>();
        proposals = new HashMap<>();

        slot_in = LOG_INITIAL;
        slot_out = LOG_INITIAL;
        leader_tick_miss = 0; //starts with no leader
        leader = null;

    }


    @Override
    public void init() {
        proposals.put(LEADER_ELECTION_SLOT, new ProposedSlot(new HashSet<>(), null));
        proposals.put(GARBAGE_SLOT, new ProposedSlot(new HashSet<>(), null));
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
     * If this server has garbage-collected this slot, it should return {@link
     * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen
     * command for this slot. If this server has both accepted and chosen a
     * command for this slot, it should return {@link PaxosLogSlotStatus#CHOSEN}.
     * <p>
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum the index of the log slot
     * @return the slot's status
     * @see PaxosLogSlotStatus
     */
    public PaxosLogSlotStatus status(int logSlotNum) {
        if (log.containsKey(logSlotNum)) {
            return log.get(logSlotNum).status();
        }
        return logSlotNum >= slot_out ? PaxosLogSlotStatus.EMPTY : PaxosLogSlotStatus.CLEARED; //need to add collection check
    }

    /**
     * Return the command associated with a given slot in the server's local
     * log.
     * <p>
     * If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
     * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}.
     * Otherwise, return the command this server has chosen or accepted,
     * according to {@link PaxosServer#status}.
     * <p>
     * If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this
     * method should unwrap them before returning.
     * <p>
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum the index of the log slot
     * @return the slot's contents or {@code null}
     * @see PaxosLogSlotStatus
     */
    public Command command(int logSlotNum) {
        return log.containsKey(logSlotNum) ? log.get(logSlotNum).amoCommand().command() : null;
    }

    /**
     * Return the index of the first non-cleared slot in the server's local log.
     * The first non-cleared slot is the first slot which has not yet been
     * garbage-collected. By default, the first non-cleared slot is 1.
     * <p>
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     * @see PaxosLogSlotStatus
     */
    public int firstNonCleared() {
        return 1; //until garbage collection is implemented
    }

    /**
     * Return the index of the last non-empty slot in the server's local log,
     * according to the defined states in {@link PaxosLogSlotStatus}. If there
     * are no non-empty slots in the log, this method should return 0.
     * <p>
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     * @see PaxosLogSlotStatus
     */
    public int lastNonEmpty() {
        return log.keySet().stream().max(Comparator.naturalOrder()).orElse(0);
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        if (isLeader()) {
            debugSenderMsg(sender, "ack paxos req num", Integer.toString(m.cmd().num()), "and is leader");
            if (app.alreadyExecuted(m.cmd())) {
                if (app.execute(m.cmd()) != null) {
                    send(new PaxosReply(app.execute(m.cmd())), sender);
                }
            } else if (proposals.entrySet().stream().anyMatch(
                    e -> e.getValue().entry() != null && e.getValue().entry().amoCommand().equals(m.cmd()))) {
                Map.Entry<Integer, ProposedSlot> currEntry = proposals.entrySet().stream()
                        .filter(e -> e.getValue().entry() != null && e.getValue().entry().amoCommand().equals(m.cmd()))
                        .findFirst()
                        .orElseThrow();
                send2A(currEntry.getKey(), currEntry.getValue().entry());
                /*
                LogEntry newEntry = new LogEntry(currEntry.getValue().entry());
                newEntry.seqNum(newEntry.seqNum() + 1);
                proposals.put(currEntry.getKey(), new ProposedSlot(new HashSet<>(), newEntry));
                send2A(currEntry.getKey(), newEntry);

                 */
            } else {
                int currSlot = slot_in++;
                LogEntry currEntry = new LogEntry(ballot, m.cmd(), PaxosLogSlotStatus.ACCEPTED);
                proposals.put(currSlot, new ProposedSlot(new HashSet<>(), currEntry));
                send2A(currSlot, currEntry);
            }
        }
    }

    // leader election
    private void handlePaxos1A(Paxos1A m, Address sender) {
        if (!checkLeaderAlive() && m.ballot().compareTo(ballot) >= 0) {
            debugSenderMsg(sender, "ack 1a, ballot", m.ballot().toString());
            //might have to save ballot, but I think it's fine
            ballot.seqNum(m.ballot().seqNum());
            send1B(m.ballot());
            leader = null;
        }

    }

    private void handlePaxos1B(Paxos1B m, Address sender) {
        if (currBallot(m.ballot())){
            debugSenderMsg(sender, "ack 1b", m.ballot().toString());
            if(ack1B(m, sender)) {
                //repropose with no-ops
                ballot.seqNum(ballot.seqNum() + 1);
                fillLogNoOps();
                resetProposals();
                sendHeartBeat();
            }
        }
    }


    /**
     * Takes an accepted ballot, and then adds it as a {@link PaxosLogSlotStatus#ACCEPTED} slot.
     * Send out message telling all replicas accepted/rejected the message.
     * <p>
     * P2A(slot, seq, logentry) =>
     * if (leader) =>
     * set(seq, logentry) for slot
     * send2B(slot, seq)
     *
     * @param m
     * @param sender
     */
    private void handlePaxos2A(Paxos2A m, Address sender) {
        if (!isLeader(sender)) {
            debugSenderMsg(sender, "rejected 2a; not leader");
            return;
        }
        if(currBallot(m.entry().ballot()) && setLogState(m.slot(), m.entry())){
            debugSenderMsg(sender, "ack 2a");
            send2B(m.slot(), m.entry().ballot());
        }
    }

    /**
     * Log => Map(slots, LogEntries)
     * LogEntry(Ballot, state, command)
     * Ballot -> (seqNum, address)
     *
     *
     */

    /**
     * Receive the accept/reject from the replicas.
     * P2B(slot, seq) =>
     * if (leader) =>
     * if (slot, seq) is equal =>
     * set(slot, seq) to confirmed
     *
     * @param m
     * @param sender
     */
    private void handlePaxos2B(Paxos2B m, Address sender) {
        debugSenderMsg(sender, "ack 2b");
        if (!isLeader()) {
            debugSenderMsg(sender, "ignored b/c not leader");
            return;
        }
        if (currBallot(m.ballot()) && ack2B(m, sender)) {
            setSlotChosen(m.slot());
            executeLog();
        }
    }

    private void handleHeartBeat(HeartBeat tick, Address sender) {
        if (!isLeader() && tick.leaderBallot().seqNum() >= ballot.seqNum()) {
            debugSenderMsg(sender, "heartbeat ack", tick.leaderBallot().toString());
            updateLeader(sender);
            ballot.seqNum(tick.leaderBallot().seqNum());
            catchUpLog(tick.leaderLog());
            executeLog();
//            sendHeartBeatResponse(ballot, slot_in);
        }
    }
    //  private void handleHeartBeatResponse


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onHeartBeatTimer(HeartBeatTimer ht) {
        if (isLeader()) {
            sendHeartBeat();
            set(ht, HeartBeatTimer.SERVER_TICK_MILLIS);
        } else if (!tickLeaderAndCheckAlive()) {
            //leader is dead, attempt to become leader
            send1A();
            set(ht, HeartBeatTimer.ELECTION_TICK_MILLIS);
        }
    }


    /* -------------------------------------------------------------------------
        Log Utils
       -----------------------------------------------------------------------*/
    private boolean setLogState(int slot, LogEntry e) {
        LogEntry cur = log.get(slot);
        if (cur == null || cur.ballot().compareTo(e.ballot()) < 0) {
            if (slot_in <= slot) {
                slot_in = slot + 1;
            }
            log.put(slot, e);
            debugMsg("set log state slot", Integer.toString(slot), "log entry", e.toString());
        }
        return log.get(slot).ballot().equals(e.ballot());
    }

    private void executeLog() {
        debugMsg("executing log");
        LogEntry curr = log.get(slot_out);
        while (curr != null && curr.status() == PaxosLogSlotStatus.CHOSEN) {
            if(curr.amoCommand() != null) {
                debugMsg("\texecuting log for slot", Integer.toString(slot_out));
                PaxosReply reply = new PaxosReply(app.execute(curr.amoCommand()));
                if (isLeader()) {
                    debugMsg("\tsending res for slot", Integer.toString(slot_out));
                    send(reply, curr.amoCommand().sender());
                }
            }
            curr = log.get(++slot_out);
        }
    }

    private void catchUpLog(Map<Integer, LogEntry> other) {
        int otherMax = other.keySet().stream().max(Comparator.naturalOrder()).orElse(0); //0 when empty
        int i = slot_out;
        LogEntry curr = other.get(i);
        while (curr != null && curr.status() == PaxosLogSlotStatus.CHOSEN) {
            log.put(i, curr);
            curr = other.get(++i);
        }
        for (;i <= otherMax; i++) {
            if (log.get(i) == null || other.get(i).ballot().compareTo(log.get(i).ballot()) > 0) {
                log.put(i, other.get(i));
            }
        }

        slot_in = Math.max(otherMax + 1, slot_in);
    }

    private void fillLogNoOps(){
        for(int i = slot_out; i < slot_in; i++){
            log.putIfAbsent(i, new LogEntry(ballot, null, PaxosLogSlotStatus.CHOSEN));
        }
    }

    private void setSlotChosen(int slot) {
        log.get(slot).status(PaxosLogSlotStatus.CHOSEN);
    }

    private void resetProposals(){
        proposals.clear();
        proposals.put(LEADER_ELECTION_SLOT, new ProposedSlot(new HashSet<>(), null));
        for(int i = slot_out; i < slot_in; i++){
            if(log.get(i).status() == PaxosLogSlotStatus.ACCEPTED){
                log.get(i).ballot(ballot);
                proposals.put(i, new ProposedSlot(new HashSet<>(), log.get(i)));
            }
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/

    private void send1A() {
        proposals.get(LEADER_ELECTION_SLOT).received2B().clear();
        ballot.seqNum(ballot.seqNum() + 1);
        debugMsg("thinks leader is dead, proposing with round ", Integer.toString(ballot.seqNum()));
        serverBroadcast(new Paxos1A(ballot, log));
    }

    private void send1B(Ballot leaderBallot) {
        debugMsg("sending 1B to", leaderBallot.toString());
        sendServer(new Paxos1B(leaderBallot, log), leaderBallot.sender());
    }

    private void send2A(int slot, LogEntry e) {
        debugMsg("send 2a, slot:", Integer.toString(slot), e.toString());
        Paxos2A proposal = new Paxos2A(slot, e, ballot);
        serverBroadcast(proposal);
    }

    private void send2B(int slot, Ballot b) {
        Paxos2B p2b = new Paxos2B(slot, b);
        sendServer(p2b, leader);
    }

    private void sendHeartBeat() {
        debugMsg("sending heartbeat", ballot.toString());
        serverBroadcast(new HeartBeat(ballot, log));

    }

    private boolean ack1B(Paxos1B b, Address sender) {
        proposals.get(LEADER_ELECTION_SLOT).received2B().add(sender);
        catchUpLog(b.log());
        debugMsg(Integer.toString(proposals.get(LEADER_ELECTION_SLOT).received2B().size()), "/", Integer.toString(servers.length), "ballot", ballot.toString());
        if (proposals.get(LEADER_ELECTION_SLOT).received2B().size() > servers.length / 2) {
            debugMsg("IS LEADER");
            leader = address();
        }
        return isLeader();
    }

    /**
     * @param b
     * @param sender
     * @return true if > 1/2 servers respond
     */
    private boolean ack2B(Paxos2B b, Address sender) {
        proposals.get(b.slot()).received2B().add(sender);
        return proposals.get(b.slot()).received2B().size() > servers.length / 2;
    }


    private boolean isLeader(Address a) {
        return a.equals(leader);
    }

    private boolean isLeader() {
        return this.address().equals(leader);
    }

    private boolean isReplica() {
        return !isLeader();
    }

    private boolean currBallot(Ballot b){
        return b.equals(ballot);
    }

    private boolean checkLeaderAlive(){
        return leader_tick_miss > 0;
    }

    private boolean tickLeaderAndCheckAlive() {
        leader_tick_miss--;
        return checkLeaderAlive();
    }

    private void updateLeader(Address newLeader) {
        leader_tick_miss = REPLICA_LEADER_WAIT;
        leader = newLeader;
        proposals.get(LEADER_ELECTION_SLOT).received2B().clear();
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
