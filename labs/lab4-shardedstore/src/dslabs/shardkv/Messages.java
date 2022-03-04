package dslabs.shardkv;

import dslabs.atmostonce.Request;
import dslabs.framework.Message;
import dslabs.framework.Result;
import lombok.Data;

@Data
final class ShardStoreRequest implements Message {
    private final Request request;
}

@Data
final class ShardStoreReply implements Message {
    private final Result result;
}

// Your code here...
