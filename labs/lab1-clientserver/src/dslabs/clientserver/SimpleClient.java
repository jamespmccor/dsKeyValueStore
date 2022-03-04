package dslabs.clientserver;

import com.google.common.base.Objects;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.clientserver.ClientTimer.CLIENT_RETRY_MILLIS;

/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * See the documentation of {@link Client} and {@link Node} for important
 * implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {
    private final Address serverAddress;
    // Your code here...

    private int seqNum;
    private dslabs.clientserver.Request request;
    private Result result;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public SimpleClient(Address address, Address serverAddress) {
        super(address);
        this.serverAddress = serverAddress;
        seqNum = 0;
    }

    @Override
    public synchronized void init() {
        // No initialization necessary
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        request = new dslabs.clientserver.Request(new AMOCommand(seqNum, this.address(), command));
        result = null;

        send(request, serverAddress);
        set(new ClientTimer(request), CLIENT_RETRY_MILLIS);
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
    private synchronized void handleReply(Reply m, Address sender) {
        if (Objects.equal(request.command().num(), m.result().num())) {
            result = m.result().result();
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        if (Objects.equal(request, t.request()) && result == null) {
            send(request, serverAddress);
            set(t, CLIENT_RETRY_MILLIS);
        }
    }
}
