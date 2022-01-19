package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
    static final int STARTUP_VIEWNUM = 0;
    private static final int INITIAL_VIEWNUM = 1;

    private View curView;
    private View nextView;
    private final HashMap<Address, Integer> pings;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ViewServer(Address address) {
        super(address);
        curView = new View(STARTUP_VIEWNUM, null, null);
        nextView = null;
        pings = new HashMap<>();
    }

    @Override
    public void init() {
        set(new PingCheckTimer(), PING_CHECK_MILLIS);
        // Your code here...
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePing(Ping m, Address sender) {
        // record all pings (because they are therefore alive)
        // but make sure to take the largest one or default to startup (smallest)
        pings.put(sender, Math.max(m.viewNum(),
                pings.getOrDefault(sender, STARTUP_VIEWNUM)));
        if (nextView != null && nextView.primary().equals(sender) && m.viewNum() ==
                nextView.viewNum()) {
            curView = nextView;
            nextView = null;
        }

        updateViewState();

        // no new view; we just send cur view
        if (nextView == null) {
            send(new ViewReply(curView), sender);
            return;
        }

        // view must change

        // since the only case the primary changes is when its cut off from network
        // then theres no point signalling to it that the primary has changed
        // if next != cur then just tell next its prim now
        if (!nextView.primary().equals(curView.primary()) &&
                sender.equals(nextView.primary()) ||
                // if the backup changes then the primary must exist; notify cur primary
                // of new view state before confirming new update
                nextView.backup() != null && !nextView.backup().equals(curView.backup()) &&
                        sender.equals(curView.primary())) {

            send(new ViewReply(nextView), sender);
            return;
        }
        send(new ViewReply(curView), sender);
    }

    private void handleGetView(GetView m, Address sender) {
        View v = nextView != null && sender == curView.primary() ? nextView :
                curView;
        ViewReply viewReply = new ViewReply(v);
        send(viewReply, sender);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingCheckTimer(PingCheckTimer t) {
        updateViewState();

        pings.clear();
        set(t, PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/

    private boolean updateViewState() {
        // initial state
        if (curView.viewNum() == STARTUP_VIEWNUM) {
            if (pings.size() > 0) {
                curView = nextView = new View(INITIAL_VIEWNUM,
                        pings.keySet().iterator().next(), null);
                return true;
            }
            return false;
        } else if (curView.viewNum() == INITIAL_VIEWNUM) {
            if (pings.size() > 1) {
                // then we grab alternate address
                Address backup = null;
                for (Map.Entry<Address, Integer> e : pings.entrySet()) {
                    if (!e.getKey().equals(curView.primary())) {
                        backup = e.getKey();
                        break;
                    }
                }
                curView = nextView = new View(INITIAL_VIEWNUM + 1, curView.primary(), backup);
                return true;
            }
            return false;
        }

        if (nextView != null) return false;

        // all other states are "normal"

        // first we check primary/backup up
        boolean primaryUp = curView.primary() != null &&
                pings.containsKey(curView.primary()) &&
                pings.get(curView.primary()) >= curView.viewNum();
        boolean backupUp = curView.backup() != null &&
                pings.containsKey(curView.primary()) &&
                pings.get(curView.backup()) == curView.viewNum();

        // then we grab alternate address
        Address alternate = null;
        for (Map.Entry<Address, Integer> e : pings.entrySet()) {
            if (!e.getKey().equals(curView.primary()) &&
                    !e.getKey().equals(curView.backup())) {
                alternate = e.getKey();
                break;
            }
        }

        // primary, backup down, use alternate if exists
        if (primaryUp && !backupUp && alternate != null) {
            // backup down, select new backup
            nextView = new View(curView.viewNum() + 1, curView.primary(),
                    alternate);
            return true;
        } else if (!primaryUp && backupUp) {
            // primary down, backup up, move backup to primary, always occurs
            nextView = new View(curView.viewNum() + 1, curView.backup(),
                    alternate);
            return true;
        }
        // 2 other states, primary/backup up/down simul; both we don't do anything

        return false;
    }
}
