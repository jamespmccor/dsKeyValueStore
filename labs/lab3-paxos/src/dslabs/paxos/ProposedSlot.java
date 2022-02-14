package dslabs.paxos;

import dslabs.framework.Address;
import java.util.Set;
import lombok.Data;

@Data
public class ProposedSlot {
    private final Set<Address> received2B;
    private final LogEntry entry;
}
