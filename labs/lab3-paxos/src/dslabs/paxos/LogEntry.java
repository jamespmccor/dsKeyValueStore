package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@AllArgsConstructor
@Data
public class LogEntry implements Serializable {

    private final int slot;
    private final Ballot ballot;
    private final AMOCommand amoCommand;
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
