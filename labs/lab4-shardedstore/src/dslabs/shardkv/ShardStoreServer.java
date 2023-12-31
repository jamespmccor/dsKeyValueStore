package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.paxos.PaxosServer;
import dslabs.shardmaster.ShardMaster;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.text.SimpleDateFormat;
import java.util.*;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {

    public static final boolean PRINT_DEBUG = false;
    private final static int DEFAULT_ADDRESS = 0;

    private long stateTransferMs;

    private final Address[] group;
    private final int groupId;

    private static final String PAXOS_ADDRESS_ID = "paxos";
    private Address paxosAddress;

    private ShardMaster.ShardConfig config;
    private boolean reconfig;
    private Set<Integer> thingsNeeded;

    private final Map<Integer, AMOApplication<KVStore>> shards;



    /* -------------------------------------------------------------------------
        Construction and initialization
       -----------------------------------------------------------------------*/
    ShardStoreServer(Address address, Address[] shardMasters, int numShards,
                     Address[] group, int groupId) {
        super(address, shardMasters, numShards);
        this.group = group;
        this.groupId = groupId;
        shards = new HashMap<>();
        config = new ShardMaster.ShardConfig(ShardMaster.INITIAL_CONFIG_NUM - 1);
        thingsNeeded = new HashSet<>();
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

        sendQuery(new ShardMaster.Query(-1)); //TODO:change paxosserver to readonly queries
        set(new ConfigurationTimer(), ConfigurationTimer.RETRY_MILLIS);

    }


    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
        if(m.command() instanceof AMOCommand
        && checkAMOCommand((AMOCommand) m.command())){
            debugSenderMsg(sender, "ack req " + ((AMOCommand) m.command()).num());
            processAMOCommand((AMOCommand)m.command(), false);
        } else if(m.command() instanceof ShardMove){
            if(checkShardMove((ShardMove) m.command())){
                processShardMove((ShardMove)m.command(), false);
            } else if(((ShardMove) m.command()).configNum() <= config.configNum()){
                sendShardMoveAck(((ShardMove) m.command()).group());
            }
        }
    }

    private void handleShardStoreReply(ShardStoreReply m, Address sender) {
        if(m.result() instanceof ShardMoveAck
                && checkShardMoveAck((ShardMoveAck) m.result())){
            processShardMoveAck((ShardMoveAck) m.result(), false);
        }
    }

    private void handlePaxosReply(PaxosReply m, Address sender){
        assert isShardMaster(sender);
        Result res = m.result();
        if(res instanceof ShardMaster.ShardConfig
                && checkNewConfig((ShardMaster.ShardConfig) res)){
            debugSenderMsg(sender, "ack config num ", Integer.toString(((ShardMaster.ShardConfig) res).configNum()));
            processNewConfig((ShardMaster.ShardConfig) res, false);
        }
    }

    private void handlePaxosDecision(PaxosDecision dec, Address sender){
        assert sender.equals(paxosAddress);
        Command cmd = dec.decision().command();
        if(cmd instanceof AMOCommand
        && checkAMOCommand((AMOCommand) cmd)){
            processAMOCommand((AMOCommand) cmd, true);
        } else if(cmd instanceof ShardMove
        && checkShardMove((ShardMove) cmd)){
            processShardMove((ShardMove) cmd, true);
        } else if(cmd instanceof ResultWrapper){
            Result res = ((ResultWrapper) cmd).result();
            if(res instanceof ShardMoveAck
            && checkShardMoveAck((ShardMoveAck) res)){
                processShardMoveAck((ShardMoveAck) res, true);
            } else if(res instanceof ShardMaster.ShardConfig
            && checkNewConfig((ShardMaster.ShardConfig) res)){
                processNewConfig((ShardMaster.ShardConfig) res, true);
            }
        }

    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onConfigurationTimer(ConfigurationTimer t){
        //TODO:maybe do something different if reconfig == true
        if(config.configNum() < ShardMaster.INITIAL_CONFIG_NUM){
            sendQuery(new ShardMaster.Query(ShardMaster.INITIAL_CONFIG_NUM));
        } else{
            sendQuery(new ShardMaster.Query(config.configNum() + 1));
        }
        set(t, ConfigurationTimer.RETRY_MILLIS);
    }

    private void onClientTimer(ClientTimer t){
        ShardMove move = (ShardMove) t.request().command();
        if(checkShardMove(move)){
            sendShardStore(new ShardStoreRequest(move), config.shardToGroupID().get((Integer) move.shardsToMove().keySet().toArray()[DEFAULT_ADDRESS]));
            set(t, ClientTimer.RETRY_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/

    private void processAMOCommand(AMOCommand cmd, boolean replicated){
        AMOApplication<KVStore> app = shards.get(keyToShard(((KVStore.SingleKeyCommand)cmd.command()).key()));
        if(app.alreadyExecuted(cmd)){
            if(app.execute(cmd) != null) send(new ShardStoreReply(app.execute(cmd)), cmd.sender());
            debugMsg("resending req " + cmd.num() + " to "+cmd.sender());
            return;
        }
        if(!replicated){
            handleMessage(new PaxosRequest(new AMOCommand(cmd.num(), cmd.sender(), cmd)), paxosAddress);
            return;
        }
        send(new ShardStoreReply(app.execute(cmd)), cmd.sender());
        debugMsg("sent req " + cmd.num() + " to " + cmd.sender());
    }
    private void processShardMove(ShardMove move, boolean replicated){
        if(!replicated){
            handleMessage(new PaxosRequest(
                    new AMOCommand(config.configNum(), getDefaultAddress(move.group()), move)),
                    paxosAddress);
            return;
        }
        shards.putAll(move.shardsToMove());
        thingsNeeded.removeAll(move.shardsToMove().keySet());
        sendShardMoveAck(move.group());
        if(thingsNeeded.isEmpty()){
            reconfig = false;
            debugMsg(System.currentTimeMillis() - stateTransferMs + " ms to get ShardMoves on config " + config.configNum());
        }
    }

    private void processShardMoveAck(ShardMoveAck ack, boolean replicated){
        if(!replicated){
            handleMessage(
                    new PaxosRequest(
                            new AMOCommand(config.configNum(),
                                    getDefaultAddress(ack.group()),
                                    new ResultWrapper(ack))),
                    paxosAddress);
            return;
        }
        thingsNeeded.remove(ack.group());
        if(thingsNeeded.isEmpty()){
            reconfig = false;
            debugMsg(System.currentTimeMillis() - stateTransferMs + " ms to get ShardMoveAcks on config " + config.configNum());
        }
    }

    private void processNewConfig(ShardMaster.ShardConfig newConfig, boolean replicated){
        stateTransferMs = System.currentTimeMillis();
        if(!replicated){
            handleMessage(
                    new PaxosRequest(
                            new AMOCommand(config.configNum(),
                                    shardMasters()[DEFAULT_ADDRESS],
                                    new ResultWrapper(newConfig))),
                    paxosAddress);
            return;
        }

        config = newConfig;

        if(config.groupInfo().get(groupId) == null && shards.size() == 0) return; //not in this config

        reconfig = true;
        if(config.configNum() == ShardMaster.INITIAL_CONFIG_NUM){
            for(Integer i: config.groupInfo().get(groupId).getRight()){
                shards.put(i, new AMOApplication<>(new KVStore()));
            }
            reconfig = false;
            debugMsg("set config " + ShardMaster.INITIAL_CONFIG_NUM);
            return;
        }
        if(newConfig.groupInfo().get(groupId) == null
        || shards.size() > newConfig.groupInfo().get(groupId).getRight().size()){ //Sending shards
            Set<Integer> toMove = (new HashSet<>(shards.keySet()));
            if(newConfig.groupInfo().get(groupId) != null)
                toMove.removeAll(newConfig.groupInfo().get(groupId).getRight());
            thingsNeeded.clear();
            for(Integer shardNum: toMove){
                thingsNeeded.add(newConfig.shardToGroupID().get(shardNum));
            }
            for(Integer groupIDNeeded: thingsNeeded){
                ShardMove move = new ShardMove(newConfig.configNum(), Arrays.asList(group), new HashMap<>());
                Set<Integer> shardNums = new HashSet<>(shards.keySet());
                shardNums.retainAll(newConfig.groupInfo().get(groupIDNeeded).getRight());

                for(Integer shardNum: shardNums){
                    move.shardsToMove().put(shardNum, shards.remove(shardNum));
                }
                ShardStoreRequest req = new ShardStoreRequest(move);
                sendShardStore(req, groupIDNeeded);
                set(new ClientTimer(req), ClientTimer.RETRY_MILLIS); //Maybe not needed?
            }

            debugMsg(System.currentTimeMillis() - stateTransferMs + " ms to send ShardMoves on config " + config.configNum());
            stateTransferMs = System.currentTimeMillis();

        } else if(shards.size() < newConfig.groupInfo().get(groupId).getRight().size()){ //Receiving shards
            thingsNeeded = new HashSet<>(newConfig.groupInfo().get(groupId).getRight());
            thingsNeeded.removeAll(shards.keySet());
        } else{
            reconfig = false;
        }
    }

    private boolean checkAMOCommand(AMOCommand cmd){
        return config.configNum() >= ShardMaster.INITIAL_CONFIG_NUM
                && !reconfig
                && shards.containsKey(keyToShard(((KVStore.SingleKeyCommand)cmd.command()).key()));
    }
    private boolean checkShardMove(ShardMove move){
        return reconfig
                && isCurrConfig(move.configNum());
    }
    private boolean checkShardMoveAck(ShardMoveAck ack){
        return reconfig
                && isCurrConfig(ack.configNum());
    }
    private boolean checkNewConfig(ShardMaster.ShardConfig newConfig){
        return !reconfig
                && isCurrConfig((newConfig.configNum() - 1));
    }



    private void sendQuery(ShardMaster.Query query) {
        for(Address a: shardMasters()){
            send(new PaxosRequest(new AMOCommand(-1, address(), query)), a);
        }
    }
    private void sendShardStore(Message m, Integer id) {
        for(Address a: config.groupInfo().get(id).getLeft()){
            send(m, a);
        }
    }
    private void sendShardStore(Message m, Collection<Address> id){
        for(Address a: id){
            send(m, a);
        }
    }

    private void sendShardMoveAck(Collection<Address> id){
        sendShardStore(new ShardStoreReply(new ShardMoveAck(config.configNum(), groupId)), id);
    }

    private boolean isCurrConfig(int num){
        return num == config.configNum();
    }

    private Address getDefaultAddress(Collection<Address> id){
        return (Address) id.toArray()[DEFAULT_ADDRESS];
    }
    private Address getDefaultAddress(int id){
        return (Address) config.groupInfo().get(id).getLeft().toArray()[DEFAULT_ADDRESS];
    }


    private boolean isShardMaster(Address address){
        return Arrays.asList(shardMasters()).contains(address);
    }


    /* -------------------------------------------------------------------------
    Debug
    -----------------------------------------------------------------------*/

    private void debugSenderMsg(Address sender, String... msgs) {
        debugMsg("<-", sender.toString(), String.join(" ", msgs));
    }

    private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private void debugMsg(String... msgs) {
        if (PRINT_DEBUG) {
            System.out.println(sdf2.format(new Date()) + " " + this.address().toString() + ": " + String.join(" ", msgs));
        }
    }
}
