package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    stateTransfer = false;
    viewNum = STARTUP_VIEWNUM;
    this.primary = null;
    this.backup = null;
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
    } else if (!stateTransfer && backup == null && getNewBackup() != null) {
      stateTransfer = true;
      viewNum++;
      backup = getNewBackup();
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

    if (!primaryUp) {
      // primary down, backup up, move backup to primary, always occurs
      stateTransfer = true;
      viewNum++;
      primary = backup;
      backup = getNewBackup();
      // backup down, select new backup. alternate != cv.backup check to
      // skip new view if backup is null
    } else {
      // primary up, backup down
      if (getNewBackup() != null) {
        stateTransfer = true;
        viewNum++;
        backup = getNewBackup();
      } else if (backup != null) {
        stateTransfer = true;
        viewNum++;
        backup = null;
      }
    }

    set(t, PING_CHECK_MILLIS);
  }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/

  private void timeout() {
    List<Address> remove = new ArrayList<>();
    for (Address a : pings.keySet()) {
      pings.put(a, pings.get(a) - 1);
      if (pings.get(a) <= 0) {
        remove.add(a);
      }
    }
    for (Address a : remove) {
      pings.remove(a);
    }
  }

  private ViewReply getViewReply() {
    return new ViewReply(new View(viewNum, primary, backup));
  }

  private Address getNewBackup() {
    for (Address a : pings.keySet()) {
      if (!a.equals(primary) && !a.equals(backup)) {
        return a;
      }
    }
    return null;
  }
}
