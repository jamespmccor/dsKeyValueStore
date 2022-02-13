package dslabs.paxos;

import dslabs.framework.Message;
import lombok.Data;

@Data
class Paxos2A implements Message{
    private int slot;
    private int seqNum;
    private LogEntry entry;
}

@Data
class Paxos2B implements Message {
    private Ballot ballot;
    private int slot;
}
