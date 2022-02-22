package dslabs.paxos;

import dslabs.framework.Address;
import lombok.Data;

@Data
public class Ballot implements Comparable<Ballot> {

    public static final Ballot INVALID_BALLOT = new Ballot(-1, null);

    private int seqNum;
    private Address leader;

    public Ballot(int seqNum, Address leader) {
        this.seqNum = seqNum;
        this.leader = leader;
    }
    public Ballot(Ballot b){
        this(b.seqNum, b.leader);
    }

    public int compareTo(Ballot b) {
        return seqNum != b.seqNum ? Integer.compare(seqNum, b.seqNum) :
                -leader.compareTo(b.leader);
    }
}