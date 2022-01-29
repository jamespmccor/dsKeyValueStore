package dslabs.primarybackup;

import dslabs.framework.Timer;
import dslabs.framework.Message;
import lombok.Data;

@Data
final class PingCheckTimer implements Timer {
    static final int PING_CHECK_MILLIS = 100;
}

@Data
final class PingTimer implements Timer {
    static final int PING_MILLIS = 25;
}

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    private final Request request;
}

@Data
class ServerTimer implements Timer {
    static final int SERVER_RETRY_MILLIS = 100;
}

@Data
class StateTransferTimer extends ServerTimer {
    private final StateTransfer state;
}

@Data
class ForwardTimer extends ServerTimer {
    private final int seqNum;
    private final Request request;
}

