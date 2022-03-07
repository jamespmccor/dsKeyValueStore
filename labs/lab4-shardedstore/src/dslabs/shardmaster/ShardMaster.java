package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {

  public static final int INITIAL_CONFIG_NUM = 0;

  private final Map<Integer, ShardConfig> configLog;  //configNum -> ShardConfig
  private final Map<Integer, Set<Address>> replicaGroups; //groupID -> Set<servers>
  private final int numShards;

  private int configNum;

  public ShardMaster(int numShards) {
    this.numShards = numShards;

    replicaGroups = new TreeMap<>();
    configNum = INITIAL_CONFIG_NUM - 1;
    configLog = new HashMap<>();
  }

  public interface ShardMasterCommand extends Command {

  }

  @Data
  public static final class Join implements ShardMasterCommand {

    private final int groupId;
    private final Set<Address> servers;
  }

  @Data
  public static final class Leave implements ShardMasterCommand {

    private final int groupId;
  }

  @Data
  public static final class Move implements ShardMasterCommand {

    private final int groupId;
    private final int shardNum;
  }

  @Data
  public static final class Query implements ShardMasterCommand {

    private final int configNum;

    @Override
    public boolean readOnly() {
      return true;
    }
  }

  public interface ShardMasterResult extends Result {

  }

  @Data
  public static final class Ok implements ShardMasterResult {

  }

  @Data
  public static final class Error implements ShardMasterResult {

  }

  @Data
  @Builder
  @AllArgsConstructor
  public static final class ShardConfig implements ShardMasterResult {

    private final int configNum;

    // groupId -> <group members, shard numbers>
    private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;

    //shardNum -> groupID
    private final Map<Integer, Integer> shardToGroupID;

    public ShardConfig(int configNum) {
      this.configNum = configNum;
      groupInfo = new TreeMap<>();
      shardToGroupID = new TreeMap<>();
    }
    public ShardConfig(int configNum, Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo){
      this.configNum = configNum;
      this.groupInfo = groupInfo;
      this.shardToGroupID = new TreeMap<>();
      populateShardToGroupID(this);
    }

    private void populateShardToGroupID(ShardConfig config){
      config.shardToGroupID.clear();
      for(Integer groupID: config.groupInfo.keySet()){
        for(Integer shardNum: config.groupInfo.get(groupID).getRight()){
          config.shardToGroupID.put(shardNum, groupID);
        }
      }
    }
  }

  @Override
  public Result execute(Command command) {
    if (command instanceof Join) {
      Join join = (Join) command;

      if (replicaGroups.containsKey(join.groupId())) {
        return new Error();
      }

      replicaGroups.put(join.groupId(), join.servers());
      ShardConfig config = newConfig();

      config.groupInfo().put(join.groupId(), new ImmutablePair<>(join.servers(), new HashSet<>()));
      rebalance();
      populateShardToGroupID();

      return new Ok();
    }

    if (command instanceof Leave) {
      Leave leave = (Leave) command;

      if (!replicaGroups.containsKey(leave.groupId())) {
        return new Error();
      }

      ShardConfig config = newConfig();
      replicaGroups.remove(leave.groupId());
      config.groupInfo.remove(leave.groupId);
      rebalance();
      populateShardToGroupID();

      return new Ok();
    }

    if (command instanceof Move) {
      Move move = (Move) command;

      if (!replicaGroups.containsKey(move.groupId) //bad groupID
          || move.shardNum() < 1 || move.shardNum() > numShards //bad shardNum
          || configLog.get(configNum).groupInfo.get(move.groupId).getRight()
          .contains(move.shardNum)) { //group already has shard
        return new Error();
      }

      newConfig();
      Iterator<Pair<Set<Address>, Set<Integer>>> iter = configLog.get(configNum).groupInfo().values().iterator();
      while (!iter.next().getRight().remove(move.shardNum())) {}
      configLog.get(configNum).groupInfo.get(move.groupId).getRight().add(move.shardNum);
      populateShardToGroupID();

      return new Ok();
    }

    if (command instanceof Query) {
      Query query = (Query) command;

      if (query.configNum() == -1 && configNum >= INITIAL_CONFIG_NUM || query.configNum() > configNum) {
        return configLog.get(configNum);
      } else if (!configLog.containsKey(query.configNum())) {
        return new Error();
      } else {
        return configLog.get(query.configNum());
      }
    }

    throw new IllegalArgumentException();
  }

  private ShardConfig newConfig() {
    ShardConfig config = ShardConfig.builder().configNum(++configNum).groupInfo(
        SerializationUtils.clone(configLog.getOrDefault(configNum - 1, new ShardConfig(INITIAL_CONFIG_NUM - 1)))
            .groupInfo()).shardToGroupID(new TreeMap<>()).build();
    configLog.put(configNum, config);
    return config;
  }

  private void rebalance() {
    ShardConfig config = configLog.get(configNum);
    int minFill = numShards / replicaGroups.size();
    int extraFill = numShards % replicaGroups.size();

    Set<Integer> unusedVals = IntStream.rangeClosed(1, numShards).boxed().collect(Collectors.toSet());
    List<Integer> removed = new ArrayList<>();

    // take off extra
    int i = 0;
    for (Pair<Set<Address>, Set<Integer>> p : config.groupInfo().values()) {
      Set<Integer> shards = p.getRight();
      unusedVals.removeAll(shards);
      while (shards.size() > minFill + ((extraFill - i) > 0 ? 1 : 0)) {
        int val = shards.iterator().next();
        shards.remove(val);
        removed.add(val);
      }
      i++;
    }

    // add set of unused numbers
    removed.addAll(unusedVals);

    // fill in holes
    i = 0;
    for (Pair<Set<Address>, Set<Integer>> p : config.groupInfo().values()) {
      Set<Integer> shards = p.getRight();
      while (shards.size() < minFill + ((extraFill - i) > 0 ? 1 : 0)) {
        shards.add(removed.remove(0));
      }
      i++;
    }
  }

  private void populateShardToGroupID(){
    ShardConfig config = configLog.get(configNum);
    config.shardToGroupID.clear();
    for(Integer groupID: config.groupInfo.keySet()){
      for(Integer shardNum: config.groupInfo.get(groupID).getRight()){
        config.shardToGroupID.put(shardNum, groupID);
      }
    }
  }

}

