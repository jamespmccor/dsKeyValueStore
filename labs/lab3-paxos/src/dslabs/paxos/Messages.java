package dslabs.paxos;

import dslabs.framework.Message;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
class Paxos1A implements Message {
    private final Ballot ballot;
    private final PaxosLog log;
}

@Data
class Paxos1B implements Message {
    private final Ballot ballot;
    private final PaxosLog log;
}

@Data
class Paxos2A implements Message {
    private final LogEntry entry;
    private final Ballot leaderBallot;
}

@Data
class Paxos2B implements Message {
    private final LogEntry entry;
}

@Data
class HeartBeat implements Message {
    private final Ballot leaderBallot;
    private final PaxosLog log;
}

@Data
class HeartBeatResponse implements Message {
    private final int garbageSlot;
}
