package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
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
    int maxSeq;
    int minSeq;
    int clock;

    public class Metadata {
        Object value;
        retStatus status;
        Integer n_p; //highest prepare seen
        Integer n_a;  //highest accept seen (proposal)
        Object v_a; //highest accept seen (value)

        public Metadata(Object value) {
            this.value = value;
            this.status = new retStatus(State.Pending, value);
            this.n_p = -1;
            this.n_a = -1;
            this.v_a = null;
        }
    }

    Map<Integer, Metadata> instances;
    ConcurrentLinkedQueue clq;

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
        this.maxSeq = -1;
        this.instances = new HashMap<Integer, Metadata>();
        this.clq = new ConcurrentLinkedQueue();
        this.clock = 0;

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
        if (seq > Max()) {
            this.maxSeq = seq;
        }

        if (seq >= Min()) {
            this.instances.put(seq, new Metadata(value));
            clq.add(seq);

            // Runnable newRunnable = new Paxos();
            Thread t = new Thread(this);
            t.start();
        }
    }

    @Override
    public void run(){
        //Your code here
        int seq = (int) clq.poll();
        int proposal = -1;
        mutex.lock();
        try {
            proposal = this.clock + 1;
            this.clock++;
        } finally {
            mutex.unlock();
        }
        while (this.instances.get(seq).status.state != State.Decided) {
            Object value = this.instances.get(seq).value;
            Request req = new Request(proposal, value, seq, this.clock);
            int count = 0;
            int max_proposal = proposal;
            Object max_value = value;
            for (int i = 0; i < peers.length; i++) {
                Response res;
                if (i != me){
                    res = Call("Prepare", req, i);
                }
                else {
                    res = Prepare(req);     
                }
                if (res != null) {
                    if (res.ok) {
                        count++;
                        if (res.n_a > max_proposal) {
                            max_proposal = res.n_a;
                            max_value = res.v_a;
                        }
                    }
                    if (res.clk > this.clock) {
                        this.clock = res.clk;
                    }
                }
            }
            if (count > ports.length / 2) {
                req = new Request(proposal, max_value, seq, this.clock);
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
                        if (res.ok) {
                            count++;        
                        }
                        if (res.clk > this.clock) {
                            this.clock = res.clk;
                        }
                    }
                }
                
                if (count > ports.length / 2) {
                    for (int i = 0; i < peers.length; i++) {
                        Response res;
                        if (i != me){
                            res = Call("Decide", req, i);
                        }
                        else {
                            res = Decide(req);
                        }
                        if (res != null) {
                            if (res.clk > this.clock) {
                                this.clock = res.clk;
                            }
                        }
                    }
                }
            }
        }
    }

    // RMI handler
    public Response Prepare(Request req){
        // your code here
        if (req.proposal > this.clock) {
            this.clock = req.clk;
        }
        Response r;
        Metadata m = this.instances.get(req.seq);
        if (m == null) {
            m = new Metadata(req.value);
            this.instances.put(req.seq, m);
        }
        if (req.proposal > m.n_p) {
            m.n_p = req.proposal;
            r = new Response(true, m.n_a, m.v_a, this.clock);
        }
        else {
            r = new Response(false, m.n_a, m.v_a, this.clock);
        }
        return r;
    }

    public Response Accept(Request req){
        // your code here
        if (req.proposal > this.clock) {
            this.clock = req.clk;
        }
        Response r;
        Metadata m = this.instances.get(req.seq);
        if (req.proposal >= m.n_p) {
            m.n_p = req.proposal;
            m.n_a = req.proposal;
            m.v_a = req.value;
            r = new Response(true, m.n_a, m.v_a, this.clock);
        }
        else {
            r = new Response(false, m.n_a, m.v_a, this.clock);
        }
        return r;
    }

    public Response Decide(Request req){
        // your code here
        if (req.proposal > this.clock) {
            this.clock = req.clk;
        }
        Metadata m = this.instances.get(req.seq);
        m.status = new retStatus(State.Decided, req.value);
        this.instances.put(req.seq, m);
        return new Response(true, m.n_a, m.v_a, this.clock);
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        // Your code here
        return this.maxSeq;
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
        return -1;
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
        Metadata m = this.instances.get(seq);
        if (m != null) {
            return m.status;
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
