package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // max and min
    int[] highDone;
    TreeMap<Integer, Metadata> instances;
    ArrayBlockingQueue<Integer> abq;

    public class Metadata {
        Object value;
        State state;
        Integer n_p; //highest prepare seen
        Integer n_a;  //highest accept seen (proposal)
        int clock;

        public Metadata(Object value, State state) {
            this.value = value;
            this.state = state;
            this.n_p = -1;
            this.n_a = -1;
            this.clock = 0;
        }
    }

<<<<<<< Updated upstream
    TreeMap<Integer, Metadata> instances;
    ConcurrentLinkedQueue<Integer> clq;

=======
>>>>>>> Stashed changes
    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
        this.instances = new TreeMap<Integer, Metadata>();
<<<<<<< Updated upstream
        this.clq = new ConcurrentLinkedQueue<Integer>();
        this.clock = 0;
=======
        this.abq = new ArrayBlockingQueue<>(peers.length);
>>>>>>> Stashed changes
        this.highDone = new int[peers.length];

        for (int i = 0; i < this.highDone.length; i++) {
            this.highDone[i] = -1;
        }

        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;

        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value){
        // Your code here
        if (seq > this.highDone[this.me]) {
            this.instances.put(seq, new Metadata(value, State.Pending));
            abq.add(seq);

            Thread t = new Thread(this);
            t.start();
        }
    }

    @Override
    public void run(){
        //Your code here
        Integer seq = abq.poll();
        Metadata m = this.instances.get(seq);
        int proposal = m.clock;
        while (this.instances.get(seq).state != State.Decided) {
            proposal++;
            this.instances.put(seq, m);
            Request req = new Request(proposal, m.value, seq, this.highDone[this.me], this.me);
            int count = 0;
            int max_proposal = proposal;
            Object max_value = m.value;
            for (int i = 0; i < peers.length; i++) {
                Response res;
                if (i != me){
                    res = Call("Prepare", req, i);
                }
                else {
                    res = Prepare(req);     
                }
                if (res != null) {
                    m.clock = Math.max(res.proposal, m.clock);
                    this.instances.put(seq, m);
                    if (res.highestD > this.highDone[i]) {
                        this.highDone[i] = res.highestD;
                    }
                    if(res.in == State.Decided) {
                        m.value = res.v_a;
                        m.state = State.Decided;
                        m.n_p = res.proposal;
                        this.instances.put(seq, m);
                        return;
                    }
                    if (res.ok) {
                        count++;
                    }
                    if (res.proposal > max_proposal) {
                        max_proposal = res.proposal;
                        max_value = res.v_a;
                    }
                }
            }
            if (count > peers.length / 2) {
                req = new Request(proposal, max_value, seq, this.highDone[me], this.me);
                count = 0;
                for (int i = 0; i < peers.length; i++) {
                    Response res;
                    if (i != me){
                        res = Call("Accept", req, i);
                    }
                    else {
                        res = Accept(req);
                    }
                    if (res != null) {
                        if (res.highestD > this.highDone[i]) {
                            this.highDone[i] = res.highestD;
                        }
                        if(res.in == State.Decided) {
                            m.value = res.v_a;
                            m.state = State.Decided;
                            m.n_p = res.proposal;
                            this.instances.put(seq, m);
                            return;
                        }
                        if (res.ok) {
                            count++;        
                        }
                        m.clock = Math.max(res.proposal, m.clock);
                        this.instances.put(seq, m);
                    }
                }
                
                if (count > peers.length / 2) {
                    req = new Request(proposal, max_value, seq, this.highDone[me], this.me);
                    for (int i = 0; i < peers.length; i++) {
                        Response res;
                        if (i != me) {
                            res = Call("Decide", req, i);
                        } else {
                            res = Decide(req);
                        }
                        if (res != null) {
                            m.clock = Math.max(res.proposal, m.clock);
                            this.instances.put(seq, m);
                        }
                    }
                }
            }
        }
    }

    // RMI handler
    public Response Prepare(Request req){
        // your code here
        if (req.highestD > this.highDone[req.id]) {
            this.highDone[req.id] = req.highestD;
        }
        Response r;
        Metadata m = this.instances.get(req.seq);
        if (m == null) {
            m = new Metadata(req.value, State.Pending);
            m.n_p = req.proposal;
            m.clock = req.proposal + 1;
            this.instances.put(req.seq, m);
            return new Response(true, m.n_p, req.value, this.highDone[this.me], State.Pending);
        }
        if (m.state == State.Decided) {
            return new Response(true, m.n_p, m.value, this.highDone[this.me], State.Decided);
        }
        if (req.proposal > m.n_p) {
            m.n_p = req.proposal;
            r = new Response(true, m.n_a, m.value, this.highDone[this.me], m.state);
        }
        else {
            r = new Response(false, m.clock, m.value, this.highDone[this.me], m.state);
        }
        this.instances.put(req.seq, m);
        return r;
    }

    public Response Accept(Request req){
        // your code here
        if (req.highestD > this.highDone[req.id]) {
            this.highDone[req.id] = req.highestD;
        }
        Response r;
        Metadata m = this.instances.get(req.seq);
        if (m == null) {
            m = new Metadata(req.value, State.Pending);
            m.n_p = req.proposal;
            m.clock = req.proposal + 1;
            m.n_a = req.proposal;
            this.instances.put(req.seq, m);
            return new Response(true, m.n_a, req.value, this.highDone[this.me], State.Pending);
        }
        if (req.proposal > m.n_p) {
            m.n_p = req.proposal;
        }
        if (req.proposal >= m.n_p) {
            m.n_p = req.proposal;
            m.n_a = req.proposal;
            m.value = req.value;
            r = new Response(true, m.n_a, m.value, this.highDone[this.me], m.state);
        }
        else {
            r = new Response(false, m.n_a, m.value, this.highDone[this.me], m.state);
        }
        this.instances.put(req.seq, m);
        return r;
    }

    public Response Decide(Request req){
        // your code here
        if (req.highestD > this.highDone[req.id]) {
            this.highDone[req.id] = req.highestD;
        }
        Metadata m = this.instances.get(req.seq);
        if (m == null) {
            m = new Metadata(req.value, State.Decided);
            m.n_p = Integer.MAX_VALUE;
            m.clock = req.proposal + 1;
            this.instances.put(req.seq, m);
            return new Response(true, m.n_p, req.value, this.highDone[this.me], m.state);
        }
        if (req.proposal > m.n_p) {
            m.n_p = req.proposal;
        }
        m.value = req.value;
        m.state = State.Decided;
        m.n_p = Integer.MAX_VALUE;
        this.instances.put(req.seq, m);
        return new Response(true, m.n_p, m.value, this.highDone[this.me], m.state);
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        this.highDone[this.me] = Math.max(seq, this.highDone[this.me]);
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        // Your code here
        return this.instances.lastKey();
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        // Your code here
        int min = this.highDone[0];
        for (int i = 0; i < this.highDone.length; i++) {
            if (this.highDone[i] < min) {
                min = this.highDone[i];
            }
        }
        List<Integer> seqs = new ArrayList<Integer>();
        for (int seq : this.instances.keySet()) {
            if (seq <= min) {
                seqs.add(seq);
            }
        }
        for (int seq : seqs) {
            this.instances.remove(seq);
        }
        return min+1;
    }



    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq){
        // Your code here
        if (seq <= this.highDone[this.me]) {
            return new retStatus(State.Forgotten, null);
        }
        Metadata m = this.instances.get(seq);
        if (m != null) {
            return new retStatus(m.state, m.value);
        }
        return new retStatus(State.Pending, null);
    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable(){
        return this.unreliable.get();
    }
}
