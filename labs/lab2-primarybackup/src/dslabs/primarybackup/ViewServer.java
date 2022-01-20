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

    private View deadView;
    private View confirmedView;
    private View proposedView;
    private final HashMap<Address, Integer> pings;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ViewServer(Address address) {
        super(address);
        confirmedView = new View(STARTUP_VIEWNUM, null, null);
        proposedView = null;
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
        if (proposedView != null && proposedView.primary().equals(sender) &&
                m.viewNum() == proposedView.viewNum()) {
            rotateViews();
        }

        if (confirmedView == null) {
            // dead server;
            updateViewState();
            send(new ViewReply(confirmedView), sender);
        } else if (m.viewNum() == STARTUP_VIEWNUM && proposedView == null) {
            updateViewState();
            send(new ViewReply(proposedView), sender);
        } else if (confirmedView.backup() == null && proposedView == null) {
            updateViewState();
            send(new ViewReply(proposedView), sender);
        } if (proposedView == null) {
            send(new ViewReply(confirmedView), sender);
        } else if (!proposedView.primary().equals(confirmedView.primary()) &&
                // view must change

                // since the only case the primary changes is when its cut off from network
                // then theres no point signalling to it that the primary has changed
                // if next != cur then just tell next its prim now
                sender.equals(proposedView.primary()) ||
                // if the backup changes then the primary must exist; notify cur primary
                // of new view state before confirming new update
                proposedView.backup() != null &&
                        !proposedView.backup().equals(confirmedView.backup()) &&
                        sender.equals(confirmedView.primary())) {

            send(new ViewReply(proposedView), sender);
        } else {
            send(new ViewReply(confirmedView), sender);
        }
    }

    private void handleGetView(GetView m, Address sender) {
        ViewReply viewReply = new ViewReply(
                confirmedView != null ? confirmedView :
                deadView);
        send(viewReply, sender);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingCheckTimer(PingCheckTimer t) {
        timeout();
        updateViewState();

        set(t, PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/

    private void timeout() {
        List<Address> remove = new ArrayList<>();
        for (Address addr : pings.keySet()) {
            pings.put(addr, pings.get(addr) - 1);
            if (pings.get(addr) <= 0) {
                remove.add(addr);
            }
        }
        for (Address a: remove) {
            pings.remove(a);
        }
    }

    private void rotateViews() {
        deadView = null;
        confirmedView = proposedView;
        proposedView = null;
    }

    private void setCurNextViews(View v) {
        confirmedView = proposedView = v;
    }

    private void updateViewState() {
        if (proposedView != null) {
            return;
        }

        // dead view
        if (deadView != null) {
            // has prim/backup
            if (pings.containsKey(deadView.primary())) {
                setCurNextViews(new View(deadView.viewNum() + (deadView.backup() == null ? 0 : 1), deadView.primary(), null));
            } else if (deadView.backup() != null && pings.containsKey(deadView.backup())) {
                setCurNextViews(new View(deadView.viewNum() + 1, deadView.backup(), null));
            }
            deadView = null;
            return;
        }

        // initial state
        if (confirmedView.viewNum() == STARTUP_VIEWNUM && pings.size() > 0) {
            setCurNextViews(new View(INITIAL_VIEWNUM, pings.keySet().iterator().next(),
                    null));
            return;
        } else if (confirmedView.viewNum() == INITIAL_VIEWNUM) {
            // then we grab alternate address
            Address backup = null;
            for (Map.Entry<Address, Integer> e : pings.entrySet()) {
                if (!e.getKey().equals(confirmedView.primary())) {
                    backup = e.getKey();
                    break;
                }
            }
            if (backup != null)
                setCurNextViews(new View(INITIAL_VIEWNUM + 1, confirmedView.primary(), backup));
            return;
        }

        // all other states are "normal"

        // first we check primary/backup up
        boolean primaryUp = confirmedView.primary() != null &&
                pings.containsKey(confirmedView.primary());
        boolean backupUp = confirmedView.backup() != null &&
                pings.containsKey(confirmedView.backup());

        // then we grab alternate address
        Address alternate = null;
        for (Map.Entry<Address, Integer> e : pings.entrySet()) {
            if (!e.getKey().equals(confirmedView.primary()) &&
                    !e.getKey().equals(confirmedView.backup())) {
                alternate = e.getKey();
                break;
            }
        }


        // primary, backup down, use alternate if exists
        if (primaryUp && !backupUp && alternate != confirmedView.backup()) {
            // backup down, select new backup
            confirmedView = proposedView = new View(confirmedView.viewNum() + 1, confirmedView.primary(),
                    alternate);
        } else if (!primaryUp && backupUp) {
            // primary down, backup up, move backup to primary, always occurs
            confirmedView = proposedView = new View(confirmedView.viewNum() + 1, confirmedView.backup(),
                    alternate);
        } else if (!primaryUp && !backupUp) {
            deadView = confirmedView;
        }
        // 2 other states, primary/backup up/down simul; both we don't do anything
    }
}
