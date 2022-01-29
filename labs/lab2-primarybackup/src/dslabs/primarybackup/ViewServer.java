package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {

  static final int STARTUP_VIEWNUM = 0;
  private static final int INITIAL_VIEWNUM = 1;

  private boolean stateTransfer;
  private int viewNum;
  private Address primary;
  private Address backup;
  private final HashMap<Address, Integer> pings;

  /* -------------------------------------------------------------------------
      Construction and Initialization
     -----------------------------------------------------------------------*/
  public ViewServer(Address address) {
    super(address);
    viewNum = STARTUP_VIEWNUM;
    pings = new HashMap<>();
  }

  @Override
  public void init() {
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
  }

  /* -------------------------------------------------------------------------
      Message Handlers
     -----------------------------------------------------------------------*/
  private void handlePing(Ping m, Address sender) {
    // record all pings (because they are therefore alive)
    // but make sure to take the largest one or default to startup (smallest)
    pings.put(sender, 2);
    // rotate view if primary of next view confirms
    if (stateTransfer && primary.equals(sender) && m.viewNum() == viewNum) {
      stateTransfer = false;
    }

    if (!stateTransfer && viewNum == STARTUP_VIEWNUM) {
      stateTransfer = true;
      viewNum = INITIAL_VIEWNUM;
      primary = sender;
    } else if (!stateTransfer && backup == null) {
      setNewState(primary, getNewBackup());
    }

    send(getViewReply(), sender);
  }

  private void handleGetView(GetView m, Address sender) {
    send(getViewReply(), sender);
  }

  /* -------------------------------------------------------------------------
      Timer Handlers
     -----------------------------------------------------------------------*/
  private void onPingCheckTimer(PingCheckTimer t) {
    timeout();

    boolean primaryUp = primary != null && pings.containsKey(primary);
    boolean backupUp = backup != null && pings.containsKey(backup);

    if (stateTransfer || primaryUp && backupUp || !primaryUp && !backupUp) {
      set(t, PING_CHECK_MILLIS);
      return;
    }

    setNewState(primaryUp ? primary : backup, getNewBackup());
    set(t, PING_CHECK_MILLIS);
  }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/

  // assumes primary always exists
  private void setNewState(Address primary, Address backup) {
    if (this.primary.equals(primary) &&
        (this.backup == null && backup == null ||
            this.backup != null && this.backup.equals(backup) ||
            backup != null && backup.equals(this.backup))) {
      return;
    }
    stateTransfer = true;
    viewNum++;
    this.primary = primary;
    this.backup = backup;
  }

  private void timeout() {
    pings.replaceAll((k, v) -> --v);
    pings.values().removeIf(v -> v <= 0);
  }

  private ViewReply getViewReply() {
    return new ViewReply(new View(viewNum, primary, backup));
  }

  private Address getNewBackup() {
    return pings.keySet().stream()
        .filter(a -> !a.equals(primary) && !a.equals(backup))
        .findFirst().orElse(null);
  }
}
