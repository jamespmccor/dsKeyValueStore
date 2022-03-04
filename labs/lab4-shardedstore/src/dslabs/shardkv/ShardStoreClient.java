package dslabs.shardkv;

import dslabs.atmostonce.Request;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import dslabs.shardmaster.ShardMaster;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Set;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
    private ShardMaster.ShardConfig curConfig;
    private Map<Integer, Set<Address>> shardMappings;

    private int seqNum;
    private ShardStoreRequest request;
    private Result reply;
    private int resendCount;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ShardStoreClient(Address address, Address[] shardMasters,
                            int numShards) {
        super(address, shardMasters, numShards);
        seqNum = 0;
        resendCount = 0;
        request = null;
        reply = null;
    }

    @Override
    public synchronized void init() {
        broadcastToShardMasters(new ShardStoreRequest(new ShardMaster.Query(-1)));
        set(new ConfigurationTimer(), ConfigurationTimer.RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
        request = new ShardStoreRequest(new Request(seqNum, this.address(), command));
        reply = null;

        if (curConfig != null) {
            broadcast(request, commandToReplicaGroup(command));
        } else {
            broadcastToShardMasters(new ShardStoreRequest(new ShardMaster.Query(-1)));
        }
        set(new ClientTimer(request), ClientTimer.RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        return reply != null;
    }


    @Override
    public synchronized Result getResult() throws InterruptedException {
        while (reply == null) {
            this.wait();
        }
        seqNum++;
        return reply;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleShardStoreReply(ShardStoreReply m,
                                                    Address sender) {
        if (m.result() instanceof ShardMaster.ShardConfig) {
            ShardMaster.ShardConfig config = (ShardMaster.ShardConfig) m.result();
            if (curConfig == null || config.configNum() > curConfig.configNum()) {
                curConfig = config;
                shardMappings.clear();

                for (Pair<Set<Address>, Set<Integer>> e: curConfig.groupInfo().values()) {
                    for (int i: e.getRight()) {
                        shardMappings.put(i, e.getLeft());
                    }
                }
            }
        }
    }

    // Your code here...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
    }

    private synchronized void onConfigurationTimer(ConfigurationTimer t) {
        broadcastToShardMasters(new ShardStoreRequest(new ShardMaster.Query(-1)));
        set(t, ConfigurationTimer.RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
    Helpers
   -----------------------------------------------------------------------*/

    private Set<Address> commandToReplicaGroup(Command command) {
        if (command instanceof KVStore.SingleKeyCommand) {
            KVStore.SingleKeyCommand skc = ((KVStore.SingleKeyCommand) command);
            int shard = keyToShard(skc.key());
            return shardMappings.get(shard);
        } else {
            throw new Error('unsupported operation');
        }
    }

}
