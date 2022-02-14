package dslabs.paxos;

import dslabs.framework.Message;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
class Paxos2A implements Message {
    private final int slot;
    private final LogEntry entry;
}

@Data
class Paxos2B implements Message {
    private final int slot;
    private final Ballot ballot;
}
