package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
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

