package dslabs.shardkv;

import dslabs.framework.Command;
import dslabs.framework.Message;
import lombok.Data;

@Data
public final class PaxosDecision implements Message {
    private final Command decision;
}
