package dslabs.paxos;

import dslabs.atmostonce.Request;
import dslabs.framework.Message;
import lombok.Data;

@Data
public final class PaxosRequest implements Message {
    private final Request cmd;
}
