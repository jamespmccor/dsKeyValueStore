package dslabs.paxos;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 1000;

    private final PaxosRequest request;
    // Your code here...
}

// Your code here...
@Data
class Paxos2ATimer implements Timer{
    private final int slot;
    private final LogEntry entry;
}