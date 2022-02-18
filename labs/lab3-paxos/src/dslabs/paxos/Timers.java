package dslabs.paxos;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    private final PaxosRequest request;
    // Your code here...
}

@Data
class HeartBeatTimer implements Timer {
    static final int TICK_MILLIS = 50;
}