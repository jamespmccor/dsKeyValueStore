package dslabs.primarybackup;

import dslabs.atmostonce.Request;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
    private final Address viewServer;

    private int seqNum;
    private dslabs.primarybackup.Request request;
    private Result result;
    private View currView;
    private int resendCount;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PBClient(Address address, Address viewServer) {
        super(address);
        this.viewServer = viewServer;
        currView = null;
        resendCount = 0;
    }

    @Override
    public synchronized void init() {
        send(new GetView(), viewServer);
        set(new ClientTimer(null), ClientTimer.CLIENT_RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        assert this.address() != null;
        request = new dslabs.primarybackup.Request(new Request(seqNum, this.address(), command));
        result = null;

        if(currView != null && currView.primary() != null) {
            send(request, currView.primary());
        } else{
            send(new GetView(), viewServer);
        }
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
    private synchronized void handleReply(Reply m, Address sender) {
        if (request.amoCommand().num() == m.amoResult().num()) {
            result = m.amoResult().result();
            resendCount = 0;
            notify();
        }
    }

    private synchronized void handleViewReply(ViewReply m, Address sender) {
        currView = m.view();
    }

    private synchronized void handleViewError(ViewError err, Address sender){
        currView = null;
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        if(currView == null || currView.primary() == null){
            send(new GetView(), viewServer);
            set(t, ClientTimer.CLIENT_RETRY_MILLIS);
        } else if (request.equals(t.request()) && result == null) {
            if(++resendCount >= 2){
                send(new GetView(), viewServer);
            }
            send(request, currView.primary());
            set(t, ClientTimer.CLIENT_RETRY_MILLIS);
        }
    }
}
