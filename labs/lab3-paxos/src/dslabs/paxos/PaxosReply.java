package dslabs.paxos;

import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import dslabs.framework.Result;
import lombok.Data;

@Data
public final class PaxosReply implements Message {
    private final Result result;
}
