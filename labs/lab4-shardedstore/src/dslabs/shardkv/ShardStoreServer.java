package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.Request;
import dslabs.framework.Address;
import dslabs.kvstore.KVStore;
import dslabs.paxos.PaxosServer;
import dslabs.shardmaster.ShardMaster;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.*;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
    private final Address[] group;
    private final int groupId;

    private static final String PAXOS_ADDRESS_ID = "paxos";
    private Address paxosAddress;

    private ShardMaster.ShardConfig config;
    private boolean reconfig;
    private Set<Integer> shardsNeeded;

    private final Map<Integer, AMOApplication<KVStore>> shards;
    private final AMOApplication<ShardMaster> shardMaster;



    /* -------------------------------------------------------------------------
        Construction and initialization
       -----------------------------------------------------------------------*/
    ShardStoreServer(Address address, Address[] shardMasters, int numShards,
                     Address[] group, int groupId) {
        super(address, shardMasters, numShards);
        this.group = group;
        this.groupId = groupId;

        if(Arrays.asList(shardMasters).contains(address)){
            shards = null;
            shardMaster = new AMOApplication<>(new ShardMaster(numShards));
            config = null;
            shardsNeeded = null;
        } else {
            shards = new HashMap<>();
            shardMaster = null;
            config = null;
            shardsNeeded = new HashSet<>();
        }
        reconfig = false;



    }

    @Override
    public void init() {
        // Setup Paxos
        paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);

        Address[] paxosAddresses = new Address[group.length];
        for (int i = 0; i < paxosAddresses.length; i++) {
            paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
        }

        PaxosServer paxosServer =
                new PaxosServer(paxosAddress, paxosAddresses, address());
        addSubNode(paxosServer);
        paxosServer.init();

        sendCommand(new Request(0, address(), new ShardMaster.Query(-1))); //TODO:change paxosserver to readonly queryies
        set(new ConfigurationTimer(), ConfigurationTimer.RETRY_MILLIS);

    }


    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
        process(m.request().command(), false);
    }

    // Your code here...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onConfigurationTimer(ConfigurationTimer t){
        //TODO:maybe do something different if reconfig == true
        if(config == null){
            sendCommand(new ShardMaster.Query(-1));
        } else{
            sendCommand(new ShardMaster.Query(config.configNum()));
        }
        set(t, ConfigurationTimer.RETRY_MILLIS);

    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    private void sendCommand(Request cmd) {
        if (cmd.command() instanceof ShardMaster.Query) {
            for(Address a: shardMasters()){
                send(new ShardStoreRequest(cmd), a);
            }
        }

    }
}
