package paxos;
import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the request message for each RMI call.
 * Hint: You may need the sequence number for each paxos instance and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 * Hint: Easier to make each variable public
 */
public class Request implements Serializable {
    static final long serialVersionUID=1L;
    // Your data here
    int proposal;
    Object value;
    int seq;
    int clk;
    int highestD;
    int id;

    // Your constructor and methods here
    public Request(int proposal, Object value, int seq, int clk, int highestD, int id) {
        this.proposal = proposal;
        this.value = value;
        this.seq = seq;
        this.clk = clk;
        this.highestD = highestD;
        this.id = id;
    }
}
