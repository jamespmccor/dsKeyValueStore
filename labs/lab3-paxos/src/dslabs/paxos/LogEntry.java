package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import lombok.Data;

@Data
public class LogEntry {
    private int seqNum;
    private AMOCommand amoCommand;
    private PaxosLogSlotStatus status;
}
