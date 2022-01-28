package dslabs.primarybackup;

import com.sun.source.tree.Tree;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;

import java.util.HashSet;
import java.util.Set;

import dslabs.atmostonce.*;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {

  private final Address viewServer;

  // Your code here...
  private View currView;
  private AMOApplication<Application> app;
  private boolean stateTransfer;
  private Set<Request> forwardedRequests;

  /* -------------------------------------------------------------------------
      Construction and Initialization
     -----------------------------------------------------------------------*/
  PBServer(Address address, Address viewServer, Application app) {
    super(address);
    this.viewServer = viewServer;

    // Your code here...
    currView = new View(ViewServer.STARTUP_VIEWNUM, null, null);
    this.app = new AMOApplication<>(app);
    forwardedRequests = new HashSet<>();
  }

  @Override
  public void init() {
    // Your code here...
    set(new PingTimer(), PingTimer.PING_MILLIS);
  }

  /* -------------------------------------------------------------------------
      Message Handlers
     -----------------------------------------------------------------------*/
  private void handleRequest(Request m, Address sender) {
    if (isPrimary()) {
      if (!hasBackup()) {
        AMOResult result = app.execute(m.amoCommand());
        if (result != null) {
          send(new Reply(result), m.amoCommand().sender());
        }
      } else {
        send(new Forward(m.amoCommand()), currView.backup());
        forwardedRequests.add(m);
        set(new ForwardTimer(m), ServerTimer.SERVER_RETRY_MILLIS);
      }
    } else {
      send(new ViewError(), sender);
    }
  }

  private void handleViewReply(ViewReply m, Address sender) {
    // Your code here...
    if (m.view().viewNum() > currView.viewNum()) {
      if (m.view().primary().equals(this.address())
          && m.view().backup() != null) {

        stateTransfer = true;
        StateTransfer state = new StateTransfer(m.view(), this.app);
        send(state, m.view().backup());
        set(new StateTransferTimer(state), ServerTimer.SERVER_RETRY_MILLIS);
      }
      currView = m.view();
    }
  }

  private void handleForwardAck(ForwardAck ack, Address sender) {
    if (isPrimary() && sender.equals(currView.backup())) {
      AMOResult result = app.execute(ack.amoCommand());
      if (result != null) {
        send(new Reply(result), ack.amoCommand().sender());
      }
    } else {
      send(new ViewError(), sender);
    }
  }

  private void handleForward(Forward forward, Address sender) {
    if (isBackup()
        && sender.equals(currView.primary())) {
      app.execute(forward.amoCommand());
      send(new ForwardAck(forward.amoCommand()), sender);
    } else {
      send(new ViewError(), sender);
    }
  }

  //new backup just copies App
  private void handleStateTransfer(StateTransfer state, Address sender) {
    if (hasBackup()
        && sender.equals(currView.primary())
        && state.view().viewNum() == currView.viewNum()) {

      app = state.app();
      send(new StateTransferAck(), sender);
    }
  }

  //new backup successfully transferred state, clear recorded requests
  private void handleStateTransferAck(StateTransferAck ack, Address sender) {
    if (isPrimary()
        && sender.equals(currView.backup())) {

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
    send(new Ping(currView.viewNum()), viewServer);
    set(t, PingTimer.PING_MILLIS);
  }

  private void onStateTransferTimer(StateTransferTimer t) {
    if (stateTransfer) {
      send(t.state(), t.state().view().backup());
      set(t, ServerTimer.SERVER_RETRY_MILLIS);
    }
  }

  private void onForwardTimer(ForwardTimer t) {
    if (hasBackup() && forwardedRequests.contains(t.request())) {
      send(t.request(), currView.backup());
      set(t, ServerTimer.SERVER_RETRY_MILLIS);
    }
  }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
  // Your code here...

  private boolean isPrimary() {
    return this.address().equals(currView.primary());
  }

  private boolean isBackup() {
    return this.address().equals(currView.backup());
  }

  private boolean hasBackup() {
    return currView.backup() != null;
  }
}
