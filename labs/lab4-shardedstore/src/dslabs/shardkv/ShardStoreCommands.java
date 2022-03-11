package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import lombok.Data;
import org.checkerframework.checker.units.qual.C;

import java.util.Collection;
import java.util.Map;


@Data
final class ShardMove implements Command {
    private final int configNum;
    private final Collection<Address> group;
    private final Map<Integer, AMOApplication<KVStore>> shardsToMove;
}

@Data
final class ShardMoveAck implements Result {
    private final int configNum;
    private final int group;
}

@Data
final class ResultWrapper implements Command {
    private final Result result;
}

//@Data
//final class NewConfig implements Command {
//
//}


