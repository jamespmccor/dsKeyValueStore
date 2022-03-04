package dslabs.shardkv;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int RETRY_MILLIS = 100;
    private final ShardStoreRequest request;
}

@Data
final class ConfigurationTimer implements Timer {
    static final int RETRY_MILLIS = 100;
}

// Your code here...
