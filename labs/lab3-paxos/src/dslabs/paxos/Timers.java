package dslabs.paxos;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {

  static final int CLIENT_RETRY_MILLIS = 250;

  private final PaxosRequest request;
}

@Data
class HeartBeatTimer implements Timer {
  static final int SERVER_TICK_MILLIS = 75;
}

