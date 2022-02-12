package dslabs.paxos;

import dslabs.framework.Address;
import lombok.Data;

@Data
public class Ballot implements Comparable<Ballot> {
    private int seqNum;
    private Address sender;

    public int compareTo(Ballot b) {
        return seqNum != b.seqNum ? Integer.compare(seqNum, b.seqNum) :
                sender.compareTo(b.sender);
    }
}