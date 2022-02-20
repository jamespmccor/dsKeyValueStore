package dslabs.paxos;

import dslabs.framework.Address;
import lombok.Data;

@Data
public class Ballot implements Comparable<Ballot> {
    private int seqNum;
    private Address sender;

    public Ballot(int seqNum, Address sender) {
        this.seqNum = seqNum;
        this.sender = sender;
    }
    public Ballot(Ballot b){
        this(b.seqNum, b.sender);
    }

    public int compareTo(Ballot b) {
        return seqNum != b.seqNum ? Integer.compare(seqNum, b.seqNum) :
                sender.compareTo(b.sender);
    }
}