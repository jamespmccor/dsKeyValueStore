package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class LogEntry {
    private Ballot ballot;
    private AMOCommand amoCommand;
    private PaxosLogSlotStatus status;

    public LogEntry(LogEntry l) {
        ballot = l.ballot;
        amoCommand = l.amoCommand;
        status = l.status;
    }
}
