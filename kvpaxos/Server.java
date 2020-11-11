package kvpaxos;
import paxos.Paxos;
import paxos.State;
import paxos.Paxos.Metadata;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
// You are allowed to call Paxos.Status to check if agreement was made.

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

public class Server implements KVPaxosRMI {

    ReentrantLock mutex;
    Registry registry;
    Paxos px;
    int me;

    String[] servers;
    int[] ports;
    KVPaxosRMI stub;

    // Your definitions here
    int seq;
    TreeMap<Integer, Op> instances;


    public Server(String[] servers, int[] ports, int me){
        this.me = me;
        this.servers = servers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.px = new Paxos(me, servers, ports);
        // Your initialization code here

        seq = 0;
        instances = new TreeMap<>();


        try{
            System.setProperty("java.rmi.server.hostname", this.servers[this.me]);
            registry = LocateRegistry.getRegistry(this.ports[this.me]);
            stub = (KVPaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("KVPaxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }
    
    public Op runPaxos(Request req) {
    	if(instances.size() > 0) seq = instances.lastKey()+1;
    	Op data;
    	px.Start(seq, req.data);
    	data = wait(seq);
    	
    	while(data != req.data) {
    		seq++;
    		px.Start(seq, req.data);
        	data = wait(seq);
    	}
    	instances.put(seq, data);
    	
    	return data;
    }


    // RMI handlers
    public Response Get(Request req){
        // Your code here
    	Op data = runPaxos(req);
    	for(int i : instances.descendingKeySet()) {
    		Op tempData = instances.get(i);
    		if(tempData.key.equals(data.key) && tempData.op.equals("Put")) {
    			return new Response(new Op("Get", seq, req.data.key, tempData.value));
    		}
    	}
    	return new Response(new Op("Get", seq, req.data.key, null));
    }

    public Response Put(Request req){
        // Your code here
    	Op data = runPaxos(req);
    	return new Response(data);

    }
    
    public Op wait (int seq) {
    	int to =10;
        while(true){
            Paxos.retStatus ret = px.Status(seq);
            if(ret.state == State.Decided)
                return (Op) ret.v;
            try{
                Thread.sleep(to);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if(to < 1000)
                to = to*2;
        }	
    }


}
