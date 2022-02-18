package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {

    public static boolean PRINT_DEBUG = false;

    private final Address[] servers;

    private int seqNum;
    private PaxosRequest request;
    private Result result;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
    }

    @Override
    public synchronized void init() {

    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        request = new PaxosRequest(new AMOCommand(seqNum, this.address(), command));
        result = null;

        broadcast(request, servers);
        set(new ClientTimer(request), ClientTimer.CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        return result != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        while (result == null) {
            this.wait();
        }
        seqNum++;
        return result;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        debugSenderMsg(sender,"ack msg", m.result() == null ? "null" : Integer.toString(m.result().num()));
        if (request.cmd().num() == m.result().num()) {
            result = m.result().result();
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        if (request.equals(t.request()) && result == null) {
            debugMsg("client resend", Integer.toString(t.request().cmd().num()));
            broadcast(request, servers);
            set(t, ClientTimer.CLIENT_RETRY_MILLIS);
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
