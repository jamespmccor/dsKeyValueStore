package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import dslabs.framework.testing.LocalAddress;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {

    public static boolean PRINT_DEBUG = true;

    /**
     * All servers in the Paxos group, including this one.
     */
    private final Address[] servers;

    private Address leader;
    private final Map<Integer, LogEntry> log;
    private final Ballot ballot;
    private AMOApplication<Application> app;
    private Map<Integer, Set<Integer>> received2B;
    private int slot_out; // leader puts new proposals here
    private int slot_in; // first unexecuted slot


    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // Your code here...
        app = new AMOApplication<>(app);
        ballot = new Ballot(0, this.address());
        log = new HashMap<>();
        received2B = new HashMap<>();

        //TEMP BEFORE LEADER ELECTIONS
        leader = servers[0];
        debugMsg("set leader", leader.toString());


    }


    @Override
    public void init() {
        // Your code here...
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
        return PaxosLogSlotStatus.EMPTY; //need to add collection check
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
        return log.containsKey(logSlotNum) ? log.get(logSlotNum).amoCommand() : null;
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
        if (!isLeader()) {
            return;
        }
        // broadcast(Ballot, logEntry

    }

    // leader election
    private void handlePaxos1A(Paxos2A m, Address sender) {
        debugSenderMsg(sender, "ack 1a");
    }

    private void handlePaxos1B(Paxos2B m, Address sender) {
        debugSenderMsg(sender, "ack 1b");

    }


    /**
     * Takes an accepted ballot, and then adds it as a {@link PaxosLogSlotStatus#ACCEPTED} slot.
     * Send out message telling all replicas accepted/rejected the message.
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
        // broadcast 2B once accepted?
    }

    /**
     * Receive the accept/reject from the sender.
     *
     * @param m
     * @param sender
     */
    private void handlePaxos2B(Paxos2B m, Address sender) {
        debugSenderMsg(sender, "ack 2b");

    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/

    // chain
    //    - propose
    //    -
    private void acceptMessage() {

    }

    private void send2A(PaxosRequest req){
        Paxos2A proposal = new Paxos2A(new LogEntry())
    }

    private void send2B(){

    }

    private boolean isLeader(Address a) {
        return a.equals(leader);
    }
    private boolean isLeader(){
        return this.address().equals(leader);
    }
    private boolean isReplica() {
        return !isLeader();
    }

    /* -------------------------------------------------------------------------
    Debug
    -----------------------------------------------------------------------*/
    private void debugSenderMsg(Address sender, String... msgs) {
        debugMsg(sender.toString(), "->", String.join(" ", msgs));
    }

    private void debugMsg(String... msgs) {
        if (PRINT_DEBUG) {
            System.out.println(this.address().toString() + ": " + String.join(" ", msgs));
        }
    }
}
