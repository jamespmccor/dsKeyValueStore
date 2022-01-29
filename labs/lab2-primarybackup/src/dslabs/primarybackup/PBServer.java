package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;

import dslabs.atmostonce.*;
import java.util.TreeMap;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {

  private final Address viewServer;

  // Your code here...
  private View curView;
  private AMOApplication<Application> app;
  private boolean stateTransfer;

  private int k = 0;
  private final TreeMap<Integer, Request> forwardedRequests;

  /* -------------------------------------------------------------------------
      Construction and Initialization
     -----------------------------------------------------------------------*/
  PBServer(Address address, Address viewServer, Application app) {
    super(address);
    this.viewServer = viewServer;

    // Your code here...
    curView = new View(ViewServer.STARTUP_VIEWNUM, null, null);
    this.app = new AMOApplication<>(app);
    forwardedRequests = new TreeMap<>();
  }

  @Override
  public void init() {
    set(new PingTimer(), PingTimer.PING_MILLIS);
  }

  /* -------------------------------------------------------------------------
      Message Handlers
     -----------------------------------------------------------------------*/
  private void handleViewReply(ViewReply m, Address sender) {
    // Your code here...
    if (m.view().viewNum() > curView.viewNum()) {
      if (m.view().primary().equals(this.address())
          && m.view().backup() != null) {

        stateTransfer = true;
        StateTransfer state = new StateTransfer(m.view(), this.app);
        send(state, m.view().backup());
        set(new StateTransferTimer(state), ServerTimer.SERVER_RETRY_MILLIS);
      }
      curView = m.view();
    }
  }

  private void handleRequest(Request m, Address sender) {
    if (!isPrimary()) {
      send(new ViewError(), sender);
      return;
    }
    if (!hasBackup()) {
//      System.out.println(
//          "p" + "//" + " " + m.amoCommand().sender().toString() + "," + m.amoCommand().num());
      AMOResult result = app.execute(m.amoCommand());
      if (result != null) {
        send(new Reply(result), m.amoCommand().sender());
      }
    } else {
      k++;
      send(new Forward(k, m.amoCommand()), curView.backup());
      forwardedRequests.put(k, m);
      set(new ForwardTimer(k, m), ServerTimer.SERVER_RETRY_MILLIS);
    }
  }

  private void handleForward(Forward forward, Address sender) {
    if (!isBackup()
        || !sender.equals(curView.primary())) {
      send(new ViewError(), sender);
      return;
    }

    if (forward.seqNum() < k + 1) {
//      System.out.println("rejected sn/k: " + forward.seqNum() + " " + k);
      return;
    }
//    System.out.println("ack sn/k: " + forward.seqNum() + " " + k);

    app.execute(forward.amoCommand());
    k = forward.seqNum();
//    System.out.println("b" + forward.seqNum() + " " + forward.amoCommand().sender().toString() + ","
//        + forward.amoCommand().num());

    send(new ForwardAck(forward.seqNum()), sender);
  }

  private void handleForwardAck(ForwardAck ack, Address sender) {
    if (!isPrimary() || !sender.equals(curView.backup())) {
      send(new ViewError(), sender);
      return;
    }

    if (!forwardedRequests.containsKey(ack.seqNum()) ) {
      return;
    }
    Request m = forwardedRequests.get(ack.seqNum());

    AMOResult result = app.execute(m.amoCommand());
//    System.out.println(
//        "p" + ack.seqNum() + " " + m.amoCommand().sender().toString() + "," + m.amoCommand().num());

    while (!forwardedRequests.isEmpty() && forwardedRequests.firstKey() <= ack.seqNum()) {
      forwardedRequests.remove(forwardedRequests.firstKey());
    }
    if (result != null) {
      send(new Reply(result), m.amoCommand().sender());
    }
  }

  //new backup just copies App
  private void handleStateTransfer(StateTransfer state, Address sender) {
    if (hasBackup()
        && sender.equals(curView.primary())
        && state.view().viewNum() == curView.viewNum()) {

      app = state.app();
      forwardedRequests.clear();
      send(new StateTransferAck(), sender);
    }
  }

  //new backup successfully transferred state, clear recorded requests
  private void handleStateTransferAck(StateTransferAck ack, Address sender) {
    if (isPrimary()
        && sender.equals(curView.backup())) {

      forwardedRequests.clear();
      stateTransfer = false;
    }
  }

  private void handleViewError(ViewError err, Address sender) {
  }


  /* -------------------------------------------------------------------------
      Timer Handlers
     -----------------------------------------------------------------------*/
  private void onPingTimer(PingTimer t) {
    send(new Ping(curView.viewNum()), viewServer);
    set(t, PingTimer.PING_MILLIS);
  }

  private void onStateTransferTimer(StateTransferTimer t) {
    if (stateTransfer) {
      send(t.state(), t.state().view().backup());
      set(t, ServerTimer.SERVER_RETRY_MILLIS);
    }
  }

  private void onForwardTimer(ForwardTimer t) {
    if (hasBackup() && forwardedRequests.containsKey(t.seqNum())) {
      send(t.request(), curView.backup());
      set(t, ServerTimer.SERVER_RETRY_MILLIS);
    }
  }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
  // Your code here...

  private boolean isPrimary() {
    return this.address().equals(curView.primary());
  }

  private boolean isBackup() {
    return this.address().equals(curView.backup());
  }

  private boolean hasBackup() {
    return curView.backup() != null;
  }
}
