package dslabs.paxos;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import dslabs.atmostonce.Request;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;

/**
 * Log handles holding the log / clearing slots with quorum. Only log can execute a message.
 * <p>
 * LOG DOES NOT HANDLE QUORUM WILL RETURN ALREADY EXECUTED == TRUE ALWAYS FOR AMOCOMMAND == NULL
 */
@Data
public class PaxosLog implements Serializable {

  public static final boolean INVARIANT_CHECKS = DebugUtils.PaxosLog_INVARIANTS;

  public static final int LOG_INITIAL = 1;

  private final Map<Integer, LogEntry> log;
  private final Multimap<Request, Integer> commandToSlot;

  private int min_slot;
  private int max_slot;

  private int min_slot_unexecuted; // smallest #'d slot kept in log that is unexecuted

  public PaxosLog() {
    log = new HashMap<>();
    commandToSlot = HashMultimap.create();

    min_slot = LOG_INITIAL;
    max_slot = LOG_INITIAL - 1;
    min_slot_unexecuted = LOG_INITIAL;
  }

  public void updateLog(int slot, LogEntry logEntry) {
    updateLog(slot, logEntry, false);
  }

  public LogEntry getAndIncrementFirstUnexecuted() {
    LogEntry l = getLog(min_slot_unexecuted);
    if (l == null || l.status() != PaxosLogSlotStatus.CHOSEN) {
      return null;
    }

    min_slot_unexecuted++;
    return l;
  }

  /**
   * @param slot
   * @param logEntry
   */
  private void updateLog(int slot, LogEntry logEntry, boolean fastForward) {
    LogEntry existingLog = log.get(slot);

    if (INVARIANT_CHECKS) {
      assert logEntry.slot() >= min_slot;
    }

    // skip validity checks if fast-forward
    if (INVARIANT_CHECKS && !fastForward) {
      if (existingLog != null) {
        assert
            logEntry.status().compareTo(existingLog.status()) > 0 || logEntry.ballot().compareTo(existingLog.ballot()) > 0 :
            "can only go in order of status || can only be overwritten if in a higher round failed: " + logEntry + "\n\n" + this;
      }
    }

    if (existingLog != null && existingLog.amoCommand() != null) {
      // we want to remove the command at the slot actually
      commandToSlot.remove(existingLog.amoCommand(), slot);
    }

    if (logEntry.amoCommand() != null) {
      commandToSlot.put(logEntry.amoCommand(), slot);
    }
    log.put(slot, logEntry);

    max_slot = Math.max(max_slot, slot);
  }

  public void confirmLog(int slot) {
    LogEntry existingLog = log.get(slot);

    // skip validity checks if fast-forward
    if (INVARIANT_CHECKS) {
      assert existingLog != null;
      assert existingLog.status() == PaxosLogSlotStatus.ACCEPTED : "can only go in order of status";
    }

    existingLog.status(PaxosLogSlotStatus.CHOSEN);

    max_slot = Math.max(max_slot, slot);
//    System.out.println("confirming log slot " + slot + " " + log.get(slot).toString());
  }

  /**
   * T/F if log currently exists in ACCEPTED/CHOSEN state (ie non-null);
   *
   * @param slot
   * @return
   */
  public boolean contains(int slot) {
    return getLog(slot) != null;
  }

  public LogEntry getLog(int slot) {
    return log.getOrDefault(slot, null);
  }

  public PaxosLogSlotStatus getLogStatus(int slot) {
    if (slot < min_slot) {
      return PaxosLogSlotStatus.CLEARED;
    } else if (slot < min_slot_unexecuted) {
      return PaxosLogSlotStatus.CHOSEN;
    } else if (slot > max_slot) {
      return PaxosLogSlotStatus.EMPTY;
    }
    LogEntry logEntry = log.get(slot);
    return logEntry != null ? logEntry.status() : PaxosLogSlotStatus.EMPTY;
  }

  public void fastForwardLog(PaxosLog other) {
    for (Map.Entry<Integer, LogEntry> e : other.log.entrySet()) {
      if (e.getKey() < min_slot) {
        continue; // skip if nothing in log
      }

      // we can guarantee something happens in this case
      LogEntry logEntry = log.get(e.getKey());

      // CLEAR state
      if (logEntry == null) {
        updateLog(e.getKey(), e.getValue(), true);
      } else {
        if (INVARIANT_CHECKS) {
          // CHOSEN state check
          if (logEntry.status() == PaxosLogSlotStatus.CHOSEN && e.getValue().status() == PaxosLogSlotStatus.CHOSEN) {
            assert (logEntry.amoCommand() == null && e.getValue().amoCommand() == null) || logEntry.amoCommand()
                .equals(e.getValue().amoCommand());
          }
        }

        // CHOSEN/ACCEPTED state
        if (logEntry.status() == PaxosLogSlotStatus.ACCEPTED && (
            e.getValue().ballot().compareTo(logEntry.ballot()) > 0
                || e.getValue().status() == PaxosLogSlotStatus.CHOSEN)) {
          updateLog(e.getKey(), e.getValue(), true);
        }
      }
    }
  }

  public void garbageCollect(int to) {
    while (min_slot < to) {
      if (INVARIANT_CHECKS) {
        assert getLogStatus(min_slot) == PaxosLogSlotStatus.CLEARED || getLogStatus(min_slot) == PaxosLogSlotStatus.CHOSEN;
      }

      if (log.get(min_slot).status() == PaxosLogSlotStatus.CLEARED) {
        continue;
      }

      LogEntry removed = log.remove(min_slot);
      if (removed.amoCommand() != null) {
        commandToSlot.remove(removed.amoCommand(), min_slot);
      }
      min_slot++;
    }
  }
  public void fillNoOps(Ballot ballot) {
    for (int i = min_slot; i < max_slot; i++) {
      // we can guarantee something happens in this case
      LogEntry logEntry = log.get(i);

      if (logEntry == null) {
        updateLog(i, new LogEntry(i, ballot, null, PaxosLogSlotStatus.ACCEPTED));
      }
    }
  }

  public int getFirstNonCleared() {
    return min_slot;
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, according to the defined states in {@link
   * PaxosLogSlotStatus}. If there are no non-empty slots in the log, this method should return 0.
   */
  public int getLastNonEmpty() {
    return max_slot < min_slot ? 0 : max_slot;
  }

  /**
   * @param amoCommand
   * @return true if null or in log
   */
  public boolean commandExistsInLog(Request amoCommand) {
    return commandToSlot.containsKey(amoCommand);
  }

  public Collection<Integer> indexesOfCommand(Request command) {
    return commandToSlot.get(command);
  }

    /* -------------------------------------------------------------------------
    Debug
    -----------------------------------------------------------------------*/
}
