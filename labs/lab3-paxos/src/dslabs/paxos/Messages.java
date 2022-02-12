package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import lombok.Data;

@Data
class Paxos2A implements Message{
    private LogEntry entry;
}

@Data
class Paxos2B implements Message {
    private Ballot ballot;
    private int logIndex;
}
