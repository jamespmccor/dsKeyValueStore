package dslabs.paxos;

import com.google.common.collect.Multimap;
import dslabs.atmostonce.AMOApplication;
import dslabs.framework.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;

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

    private int seqNum;
    private Multimap<Address, Address> votes;

    private Address leader;
    private int leader_tick_miss;

    private int slot_out; // leader puts new proposals here
    private int slot_in; // first unexecuted slot


    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // Your code here...
        this.app = new AMOApplication<>(app);
        ballot = new Ballot(0, this.address());
        log = new HashMap<>();
        proposals = new HashMap<>();

        slot_out = LOG_INITIAL;
        slot_in = LOG_INITIAL;
        leader_tick_miss = 0; //starts with no leader
        leader = null;

    }


    @Override
    public void init() {
        proposals.put(LEADER_ELECTION_SLOT, new ProposedSlot(new HashSet<>(), null));
        proposals.put(GARBAGE_SLOT, new ProposedSlot(new HashSet<>(), null));
        set(new HeartBeatTimer(), HeartBeatTimer.TICK_MILLIS);
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
        return logSlotNum >= slot_in ? PaxosLogSlotStatus.EMPTY : PaxosLogSlotStatus.CLEARED; //need to add collection check
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

        if (isLeader()) { //TODO: maybe drop if still learning?
            debugSenderMsg(sender, "ack paxos req and is leader");
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
                LogEntry newEntry = new LogEntry(currEntry.getValue().entry());
                newEntry.seqNum(newEntry.seqNum() + 1);
                proposals.put(currEntry.getKey(), new ProposedSlot(new HashSet<>(), newEntry));
                send2A(currEntry.getKey(), newEntry);
            } else {
                int currSlot = slot_out++;
                LogEntry currEntry = new LogEntry(LOG_INITIAL, m.cmd(), PaxosLogSlotStatus.EMPTY);
                proposals.put(currSlot, new ProposedSlot(new HashSet<>(), currEntry));
                send2A(currSlot, currEntry);
            }
        }

    }

    // leader election
    private void handlePaxos1A(Paxos1A m, Address sender) {
        debugSenderMsg(sender, "ack 1a, ballot", m.ballot().toString());
        if (!checkLeaderAlive() && m.ballot().compareTo(ballot) >= 0) {
            //might have to save ballot, but I think it's fine
            ballot.seqNum(m.ballot().seqNum());
            send1B(new Ballot(m.ballot().seqNum(), sender));
        }

    }

    private void handlePaxos1B(Paxos1B m, Address sender) {
        debugSenderMsg(sender, "ack 1b", m.ballot().toString());
        if (m.ballot().equals(ballot)) {
            ack1B(m, sender);
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
        debugSenderMsg(sender, "ack 2a");
        setLogState(m.slot(), m.entry());
        send2B(m.slot(), m.entry().seqNum());
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
        if (ack2B(m, sender)) {
            setSlotChosen(m.slot());
            executeLog();
        }
    }

    private void handleHeartBeat(HeartBeat tick, Address sender){
        if(tick.leaderBallot().compareTo(ballot) > 0){

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
        } else if (!checkLeaderAlive()) {
            //leader is dead, attempt to become leader
            leader_tick_miss = REPLICA_LEADER_WAIT;
            send1A();
        }
        set(ht, HeartBeatTimer.TICK_MILLIS);
    }


    /* -------------------------------------------------------------------------
        Log Utils
       -----------------------------------------------------------------------*/
    private void setLogState(int slot, LogEntry e) {
        debugMsg("set log state slot", Integer.toString(slot), "log entry", e.toString());
        // TODO: add log invariant assertions
        LogEntry cur = log.get(slot);
        if (cur == null) {
            e.status(PaxosLogSlotStatus.ACCEPTED);
            log.put(slot, e);
        } else if (cur.seqNum() < e.seqNum()) {
            e.status(PaxosLogSlotStatus.ACCEPTED);
            log.put(slot, e);
        }
//        else if (cur.seqNum() == e.seqNum()) {
//            assert false : "repeated msg when there should be none";
//             possible with unreliable tests
//        } else {
//            assert false : "invalid state";
//             possible with unreliable tests
//
//        }
    }

    private void executeLog() {
        debugMsg("executing log");
        LogEntry curr = log.get(slot_in);
        while (curr != null && curr.status() == PaxosLogSlotStatus.CHOSEN) {
            debugMsg("\texecuting log for slot", Integer.toString(slot_in));
            PaxosReply reply = new PaxosReply(app.execute(curr.amoCommand()));
            if (isLeader()) {
                debugMsg("\tsending res for slot", Integer.toString(slot_in));
                send(reply, curr.amoCommand().sender());
            }
            curr = log.get(++slot_in);
        }
    }

    private void catchUpLog(Map<Integer, LogEntry> other) {
        for (int i = slot_in; i < other.size(); i++) {
            if (!log.containsKey(i)) {
                log.put(i, other.get(i));
            } else {
                // TODO: remove when safe
                assert log.get(i).status() == PaxosLogSlotStatus.ACCEPTED;

                if (other.get(i).status() == PaxosLogSlotStatus.CHOSEN
                        || other.get(i).seqNum() > log.get(i).seqNum()) {
                    log.put(i, other.get(i));
                }
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
        debugMsg("send 2a");
        Paxos2A proposal = new Paxos2A(slot, e);
        serverBroadcast(proposal);
    }

    private void send2B(int slot, int seqNum) {
        Paxos2B p2b = new Paxos2B(slot, new Ballot(seqNum, this.address()));
        sendServer(p2b, leader);
    }

    private void sendHeartBeat(){
        ballot.seqNum(ballot.seqNum()+1);
        debugMsg("sending heartbeat", ballot.toString());
        serverBroadcast(new HeartBeat(ballot, log));

    }

    private void setSlotChosen(int slot) {
        log.get(slot).status(PaxosLogSlotStatus.CHOSEN);
    }

    private void ack1B(Paxos1B b, Address sender) {
        proposals.get(LEADER_ELECTION_SLOT).received2B().add(sender);
        catchUpLog(b.log());
        debugMsg(Integer.toString(proposals.get(LEADER_ELECTION_SLOT).received2B().size()),"/", Integer.toString(servers.length), "ballot", ballot.toString());
        if (proposals.get(LEADER_ELECTION_SLOT).received2B().size() > servers.length / 2) {
            leader = address();
        }
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

    private boolean checkLeaderAlive() {
        leader_tick_miss--;
        return leader_tick_miss > 0;
    }

    private void updateLeader(Address newLeader){
        leader_tick_miss = REPLICA_LEADER_WAIT;
        leader = newLeader;
        proposals.get(LEADER_ELECTION_SLOT).received2B().clear();
    }

    private void serverBroadcast(Message m) {
        for (Address a : servers) {
            if (a.equals(this.address())) {
                this.handleMessage(m);
            } else {
                send(m, a);
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
    Debug
    -----------------------------------------------------------------------*/
    private void debugSenderMsg(Address sender, String... msgs) {
        debugMsg("<-", sender.toString(), String.join(" ", msgs));
    }

    private void debugMsg(String... msgs) {
        if (PRINT_DEBUG) {
            System.out.println(this.address().toString() + ": " + String.join(" ", msgs));
        }
    }

}
