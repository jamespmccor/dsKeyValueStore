package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import lombok.Data;

@Data
public class LogEntry {
    private Ballot ballot;
    private AMOCommand amoCommand;
    private PaxosLogSlotStatus status;
}
