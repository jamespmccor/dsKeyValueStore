package dslabs.clientserver;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import dslabs.framework.Result;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Simple server that receives requests and returns responses.
 *
 * See the documentation of {@link Node} for important implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleServer extends Node {
    private Application app;
    private Map<Address, Reply> clientMap;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public SimpleServer(Address address, Application app) {
        super(address);

        this.app = app;
        this.clientMap = new HashMap<>();
    }

    @Override
    public void init() {
        // No initialization necessary
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender) {
        if (clientMap.containsKey(sender)) {
            if (clientMap.get(sender).sequenceNum() == m.sequenceNum()) {
                // must mean resend
                send(clientMap.get(sender), sender);
                return;
            } else if (clientMap.get(sender).sequenceNum() > m.sequenceNum()) {
                // old request, ignore
                return;
            }
        }
        Reply reply = new Reply(app.execute(m.command()), m.sequenceNum());
        clientMap.put(sender, reply);
        send(reply, sender);
    }
}
