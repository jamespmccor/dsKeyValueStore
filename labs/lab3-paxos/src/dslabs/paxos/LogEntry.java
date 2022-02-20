package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class LogEntry {

    private int slot;
    private Ballot ballot;
    private AMOCommand amoCommand;
    private PaxosLogSlotStatus status;

    public LogEntry(LogEntry l) {
        slot = l.slot;
        ballot = l.ballot;
        amoCommand = l.amoCommand;
        status = l.status;
    }

    public LogEntry(LogEntry l, PaxosLogSlotStatus status) {
        slot = l.slot;
        ballot = l.ballot;
        amoCommand = l.amoCommand;
        this.status = status;
    }

    public LogEntry(LogEntry l, Ballot ballot) {
        slot = l.slot;
        this.ballot = ballot;
        amoCommand = l.amoCommand;
        status = l.status;
    }
}
