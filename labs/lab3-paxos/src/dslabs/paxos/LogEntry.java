package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class LogEntry {
    private int seqNum;
    private AMOCommand amoCommand;
    private PaxosLogSlotStatus status;

    public LogEntry(LogEntry l) {
        seqNum = l.seqNum;
        amoCommand = l.amoCommand;
        status = l.status;
    }
}
