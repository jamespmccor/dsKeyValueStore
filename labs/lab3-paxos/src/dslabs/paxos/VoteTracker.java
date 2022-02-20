package dslabs.paxos;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import java.util.HashMap;
import java.util.Map;

/**
 * Proposals is responsible for tracking proposed log entries. It gathers ballots from servers and if a
 */
public class VoteTracker {

  public static final boolean INVARIANT_CHECK = DebugUtils.VoteTracker_INVARIANTS;

  private final PaxosLog log;
  private final Address[] servers;

  private SetMultimap<Integer, Address> votes; // slot -> addresses that voted for it

  public VoteTracker(Address[] servers, PaxosLog log) {
    this.servers = servers;
    this.log = log;

    votes = HashMultimap.create();
  }

  /**
   * Creates a log entry at the first empty slot.
   *
   * @param command
   * @return
   */
  public LogEntry createLogEntry(Ballot ballot, AMOCommand command) {
    if (INVARIANT_CHECK) {
      if (command != null) {
        assert !log.commandExistsInLog(command) :
            "duplicate entry in log " + command;
      }
    }
    LogEntry logEntry = new LogEntry(log.getLastNonEmpty() + 1, ballot, command, PaxosLogSlotStatus.ACCEPTED);
    return logEntry;
  }

  public boolean addLogEntry(LogEntry logEntry) {
    PaxosLogSlotStatus existingLogEntryStatus = log.getLogStatus(logEntry.slot());
    if (existingLogEntryStatus != PaxosLogSlotStatus.EMPTY) {
      return false;
    }
    log.updateLog(logEntry.slot(), logEntry);
    return true;
  }
  /**
   * Takes in a vote for a log entry. Ignores duplicate commands.
   *
   * @param logEntry
   * @return whether the vote was accepted into the VoteTracker
   */
  public boolean vote(LogEntry logEntry) {
    LogEntry existingLogEntry = log.getLog(logEntry.slot());
    PaxosLogSlotStatus existingLogEntryStatus = log.getLogStatus(logEntry.slot());

    switch (existingLogEntryStatus) {
      case CLEARED:
      case CHOSEN:
        // slot already used;
        return false;
      case ACCEPTED:
        if (logEntry.ballot().seqNum() < existingLogEntry.ballot().seqNum()) {
          // reject old ballots
          return false;
        } else if (logEntry.ballot().seqNum() == existingLogEntry.ballot().seqNum()) {
          if (INVARIANT_CHECK) {
            assert logEntry.amoCommand().equals(existingLogEntry.amoCommand());
          }
          // add ballot, return t/f depending on whether already there
          boolean accepted = votes.put(logEntry.slot(), logEntry.ballot().sender());
          if (accepted) {
            confirmProposedLog(logEntry.slot());
          }
          return accepted;
        } else {
          // logEntry.ballot().seqNum() > existingLogEntry.ballot().seqNum()
          // THIS CASE IS A REMOVE ALL AND THEN GOES TO EMPTY
          votes.removeAll(logEntry.slot());
          log.updateLog(logEntry.slot(), logEntry);
          votes.put(logEntry.slot(), logEntry.ballot().sender());

          if (canSetLogStateChosen(logEntry.slot())) {
            confirmProposedLog(logEntry.slot());
          }
          return true;
        }
      case EMPTY:
      default:
        throw new Error("unhandled LogEntry state");
    }
  }

  public boolean canSetLogStateChosen(int slot) {
    return votes.get(slot).size() > servers.length / 2;
  }

  public void confirmProposedLog(int slot) {
    if (!canSetLogStateChosen(slot)) {
      return;
    }

    log.confirmLog(slot);
    votes.removeAll(slot);
  }
}
