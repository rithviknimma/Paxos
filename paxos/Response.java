package paxos;
import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: You may need a boolean variable to indicate ack of acceptors and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID=2L;
    // your data here
 
    boolean ok;
    int proposal;
    Object v_a;
    int highestD;
    State in;

    // Your constructor and methods here
    public Response(boolean ok, int proposal, Object v_a, int highestD, State in) {
        this.ok = ok;
        this.proposal = proposal;
        this.v_a = v_a;
        this.highestD = highestD;
        this.in = in;
    }
}
