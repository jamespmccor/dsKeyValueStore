package dslabs.shardkv;

import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Result;
import dslabs.shardmaster.ShardMaster.Query;
import lombok.Data;

@Data
final class ShardStoreRequest implements Message {
    private final Command command;
}

@Data
final class ShardStoreReply implements Message {
    private final Result result;
}

@Data
final class ShardStoreQuery implements Message {
    private final Query query;
}
// Your code here...
