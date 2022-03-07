package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Set;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {

  private final Map<Integer, Set<Address>> shardMappings;

  private ShardMaster.ShardConfig curConfig;

  private int seqNum;
  private ShardStoreRequest request;
  private Result result;

  /* -------------------------------------------------------------------------
      Construction and Initialization
     -----------------------------------------------------------------------*/
  public ShardStoreClient(Address address, Address[] shardMasters, int numShards) {
    super(address, shardMasters, numShards);
    shardMappings = new HashMap<>();

    seqNum = 0;
    request = null;
    result = null;
  }

  @Override
  public synchronized void init() {
    getShardMasterConfig();
    set(new ConfigurationTimer(), ConfigurationTimer.RETRY_MILLIS);
  }

  /* -------------------------------------------------------------------------
      Public methods
     -----------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    request = new ShardStoreRequest(new AMOCommand(seqNum, this.address(), command));
    result = null;

    if (curConfig != null) {
      broadcast(request, commandToReplicaGroup(command));
    } else {
      getShardMasterConfig();
    }
    set(new ClientTimer(request), ClientTimer.RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    return result != null;
  }


  @Override
  public synchronized Result getResult() throws InterruptedException {
    while (result == null) {
      this.wait();
    }
    seqNum++;
    return result;
  }

  /* -------------------------------------------------------------------------
      Message Handlers
     -----------------------------------------------------------------------*/
  private synchronized void handleShardStoreReply(ShardStoreReply m, Address sender) {
    if (((AMOCommand) (request.command())).num() == ((AMOResult) (m.result())).num()) {
      result = ((AMOResult) (m.result())).result();
      notify();
    }
  }

  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    if (m.result().result() instanceof ShardMaster.ShardConfig) {
      ShardMaster.ShardConfig config = (ShardMaster.ShardConfig) m.result().result();
      if (curConfig == null || config.configNum() > curConfig.configNum()) {
        curConfig = config;
        shardMappings.clear();

        for (Pair<Set<Address>, Set<Integer>> e : curConfig.groupInfo().values()) {
          for (int i : e.getRight()) {
            shardMappings.put(i, e.getLeft());
          }
        }
      }
    } else if(m.result().result() instanceof ShardMaster.Error){ //ShardMaster not chosen initial config
    } else {
      throw new Error("unhandled");
    }
  }
  // Your code here...

  /* -------------------------------------------------------------------------
      Timer Handlers
     -----------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    if (curConfig == null) {
      getShardMasterConfig();
      set(t, ClientTimer.RETRY_MILLIS);
    } else if (request.equals(t.request()) && result == null) {
      broadcast(request, commandToReplicaGroup(((AMOCommand)t.request().command()).command()));
      set(t, ClientTimer.RETRY_MILLIS);
    }
  }

  private synchronized void onConfigurationTimer(ConfigurationTimer t) {
    getShardMasterConfig();
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
      throw new Error("unsupported operation");
    }
  }

  private void getShardMasterConfig() {
    broadcastToShardMasters(new PaxosRequest(new AMOCommand(-1, address(), new ShardMaster.Query(-1))));
  }
}
