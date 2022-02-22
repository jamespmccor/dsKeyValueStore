package dslabs.paxos;

import dslabs.framework.Address;
import lombok.Data;

@Data
public class Ballot implements Comparable<Ballot> {

    public static final Ballot INVALID_BALLOT = new Ballot(-1, null);

    private int roundNum;
    private Address sender;

    public Ballot(int roundNum, Address sender) {
        this.roundNum = roundNum;
        this.sender = sender;
    }
    public Ballot(Ballot b){
        this(b.roundNum, b.sender);
    }

    public int compareTo(Ballot b) {
        return roundNum != b.roundNum ? Integer.compare(roundNum, b.roundNum) :
                sender.compareTo(b.sender);
    }
}