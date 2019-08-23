import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.Arrays;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class SyncPrimitive implements Watcher {

    static ZooKeeper zk = null;
    static Integer mutex;

    String root;

    SyncPrimitive(String address) {
        if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
        //else mutex = new Integer(-1);
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            //System.out.println("Process: " + event.getType());
            mutex.notify();
        }
    }

    /**
     * Barrier
     */
    static public class Barrier extends SyncPrimitive {
        int size;
        String name;

        /**
         * Barrier constructor
         *
         * @param address
         * @param root
         * @param size
         */
        Barrier(String address, String root, int size) {
            super(address);
            this.root = root;
            this.size = size;

            // Create barrier node
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out
                            .println("Keeper exception when instantiating queue: "
                                    + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }

            // My node name
            try {
                name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
            } catch (UnknownHostException e) {
                System.out.println(e.toString());
            }

        }

        /**
         * Join barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */

        boolean enter() throws KeeperException, InterruptedException{
            zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);

                    if (list.size() < size) {
                        mutex.wait();
                    } else {
                        return true;
                    }
                }
            }
        }

        /**
         * Wait until all reach barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */

        boolean leave() throws KeeperException, InterruptedException{
            zk.delete(root + "/" + name, 0);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                        if (list.size() > 0) {
                            mutex.wait();
                        } else {
                            return true;
                        }
                    }
                }
        }
    }

    /**
     * Producer-Consumer queue
     */
    static public class Queue extends SyncPrimitive {

        /**
         * Constructor of producer-consumer queue
         *
         * @param address
         * @param name
         */
        Queue(String address, String name) {
            super(address);
            this.root = name;
            // Create ZK node name
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
        }

        /**
         * Add element to the queue.
         *
         * @param i
         * @return
         */

        boolean produce(int i, int id) throws KeeperException, InterruptedException{
            ByteBuffer b = ByteBuffer.allocate(8);
            byte[] value;

            // Add child with value i
            b.putInt(i);
            b.putInt(id);
            value = b.array();
            zk.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            return true;
        }


        /**
         * Remove first element from the queue.
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
        int[] consume() throws KeeperException, InterruptedException{
            int ret[] = {-1, -1};
            Stat stat = null;

            // Get the first element available
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() == 0) {
                        System.out.println("Going to wait");
                        mutex.wait();
                    } else {
                        Integer min = new Integer(list.get(0).substring(7));
                        System.out.println("List: "+list.toString());
                        String minString = list.get(0);
                        for(String s : list){
                            Integer tempValue = new Integer(s.substring(7));
                            //System.out.println("Temp value: " + tempValue);
                            if(tempValue < min) { 
                                min = tempValue;
                                minString = s;
                            }
                        }
                        System.out.println("Temporary value: " + root +"/"+ minString);
                        byte[] b = zk.getData(root +"/"+ minString,false, stat);
                        //System.out.println("b: " + Arrays.toString(b));     
                        zk.delete(root +"/"+ minString, 0);
                        ByteBuffer buffer = ByteBuffer.wrap(b);
                        ret[0] = buffer.getInt();
                        ret[1] = buffer.getInt();

                        return ret;
                    }
                }
            }
        }
    }

    static public class Lock extends SyncPrimitive {
        String pathName;
        long wait;
        int id;
        int answers[];
        Queue q;

        /**
        * Constructor of lock
        *
        * @param address
        * @param name Name of the lock node
        */
        Lock(String address, String name, long waitTime, int id, int answers[], Queue q) {
            super(address);
            this.root = name;
            this.wait = waitTime;
            this.id = id;
            this.answers = answers;
            this.q = q;
            // Create ZK node name
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
        }
        
        boolean lock() throws KeeperException, InterruptedException{
            //Step 1
            pathName = zk.create(root + "/lock-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("My path name is: "+pathName);
            //Steps 2 to 5
            return testMin();
        }
        
        boolean testMin() throws KeeperException, InterruptedException{
            while (true) {
             Integer suffix = new Integer(pathName.substring(12));
                 //Step 2 
                     List<String> list = zk.getChildren(root, false);
                     Integer min = new Integer(list.get(0).substring(5));
                     System.out.println("List: "+list.toString());
                     String minString = list.get(0);
                     for(String s : list){
                         Integer tempValue = new Integer(s.substring(5));
                         //System.out.println("Temp value: " + tempValue);
                         if(tempValue < min)  {
                             min = tempValue;
                             minString = s;
                         }
                     }
                    System.out.println("Suffix: "+suffix+", min: "+min);
                   //Step 3
                     if (suffix.equals(min)) {
                        System.out.println("Lock acquired for "+minString+"!");
                        return true;
                    }
                    //Step 4
                    //Wait for the removal of the next lowest sequence number
                    Integer max = min;
                    String maxString = minString;
                    for(String s : list){
                        Integer tempValue = new Integer(s.substring(5));
                        //System.out.println("Temp value: " + tempValue);
                        if(tempValue > max && tempValue < suffix)  {
                            max = tempValue;
                            maxString = s;
                        }
                    }
                    //Exists with watch
                    Stat s = zk.exists(root+"/"+maxString, this);
                    System.out.println("Watching "+root+"/"+maxString);
                    //Step 5
                    if (s != null) {
                        //Wait for notification
                        break;  
                    }
            }
            System.out.println(pathName+" is waiting for a notification!");
            return false;
        }

        synchronized public void process(WatchedEvent event) {
            synchronized (mutex) {
                String path = event.getPath();
                if (event.getType() == Event.EventType.NodeDeleted) {
                    System.out.println("Notification from "+path);
                    try {
                        if (testMin()) { //Step 5 (cont.) -> go to step 2 to check
                            this.compute();
                        } else {
                            System.out.println("Not lowest sequence number! Waiting for a new notification.");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        
        void compute() {
            try {
                System.out.println("I'm returning my essay, I'm student " + id + " (At lock)");
                for (int i : this.answers){
                    this.q.produce(i, id);
                }
                //Exits, which releases the ephemeral node (Unlock operation)
                //zk.delete(pathName, -1);
                System.out.println("Waiting for my score (Leaving lock)");
                System.exit(0);

            } catch (KeeperException e){
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static public class Leader extends SyncPrimitive {
    	String leader;
    	String id; //Id of the leader
    	String pathName;
        String professor;
        int answers[];
        int number_students;
        int number_questions;
        Queue q;
        
    	
   	 /**
         * Constructor of Leader
         *
         * @param address
         * @param name Name of the election node
         * @param leader Name of the leader node
         * 
         */
        Leader(String address, String name, String leader, int id, String professor, int answers[], int number_students, int number_questions, Queue q) {
            super(address);
            this.root = name;
            this.leader = leader;
            this.id = new Integer(id).toString();
            this.professor = professor;
            this.answers = answers;
            this.number_students = number_students;
            this.number_questions = number_questions;
            this.q = q;
            // Create ZK node name
            if (zk != null) {
                try {
                	//Create election znode
                    Stat s1 = zk.exists(root, false);
                    if (s1 == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }  
                    //Checking for a leader
                    Stat s2 = zk.exists(leader, false);
                    if (s2 != null) {
                        byte[] idLeader = zk.getData(leader, false, s2);
                        System.out.println("Current judge: " + professor);
                    }  
                    
                } catch (KeeperException e) {
                    System.out.println("Keeper exception when instantiating queue: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
        }
        
        boolean elect() throws KeeperException, InterruptedException{
        	this.pathName = zk.create(root + "/n-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("My name is: "+ professor);
        	return check();
        }
        
        boolean check() throws KeeperException, InterruptedException{
        	Integer suffix = new Integer(pathName.substring(12));
           	while (true) {
        		List<String> list = zk.getChildren(root, false);
        		Integer min = new Integer(list.get(0).substring(5));
        		System.out.println("List: "+list.toString());
        		String minString = list.get(0);
        		for(String s : list){
        			Integer tempValue = new Integer(s.substring(5));
        			//System.out.println("Temp value: " + tempValue);
        			if(tempValue < min)  {
        				min = tempValue;
        				minString = s;
        			}
        		}
        		System.out.println("Suffix: "+suffix+", min: "+min);
        		if (suffix.equals(min)) {
        			this.leader();
        			return true;
        		}
        		Integer max = min;
        		String maxString = minString;
        		for(String s : list){
        			Integer tempValue = new Integer(s.substring(5));
        			//System.out.println("Temp value: " + tempValue);
        			if(tempValue > max && tempValue < suffix)  {
        				max = tempValue;
        				maxString = s;
        			}
        		}
        		//Exists with watch
        		Stat s = zk.exists(root+"/"+maxString, this);
        		System.out.println("Watching "+root+"/"+maxString);
        		//Step 5
        		if (s != null) {
        			//Wait for notification
        			break;
        		}
        	}
        	System.out.println(pathName+" is waiting for a notification!");
        	return false;
        	
        }
        
        synchronized public void process(WatchedEvent event) {
            synchronized (mutex) {
            	if (event.getType() == Event.EventType.NodeDeleted) {
            		try {
            			boolean success = check();
            			if (success) {
            				compute();
            			}
            		} catch (Exception e) {e.printStackTrace();}
            	}
            }
        }
        
        void leader() throws KeeperException, InterruptedException {
			System.out.println("Become a leader: "+ professor +"!");
            //Create leader znode
            Stat s2 = zk.exists(leader, false);
            if (s2 == null) {
                zk.create(leader, id.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
            	zk.setData(leader, id.getBytes(), 0);
            }
        }
        
        void compute() {
            System.out.println("I will correct all the essays (Elected)");

            for (int i = 0; i < this.number_students; i++) {
                for (int j = 0; j < this.number_questions; j++) {
                    try{
                        int r[] = this.q.consume();
                        //System.out.println(Arrays.toString(r));
                        if(this.answers[j] == r[0]){
                            System.out.println("Student: " + r[1] + " | Question: " + (j+1) + " | Solution: Correct");
                        }
                        else{
                            System.out.println("Student: " + r[1] + " | Question: " + (j+1) + " | Solution: Incorrect");
                        }
                    } catch (KeeperException e){
                        i--;
                    } catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            }
    		System.exit(0);
        }
    }

    
    public static void main(String args[]) {
        int number_students = 2;
        Queue q = new Queue(args[1], "/app3");
        if (args[0].equals("student"))
            student(args, number_students, q);
        else if (args[0].equals("professor"))
            professor(args, number_students, q);
        else
            System.err.println("Unknown option");
    }

    public static void professor(String args[], int number_students, Queue q) {
        // Generate random integer
        Random rand = new Random();
        int idx = rand.nextInt(10);
        Integer number_questions = new Integer(args[2]);
        Integer id = new Integer(args[4]);
        int answers[] = new int [number_questions];
        String professors[] = new String[10];
        professors[0] = "Professor 1";
        professors[1] = "Professor 2";
        professors[2] = "Professor 3";
        professors[3] = "Professor 4";
        professors[4] = "Professor 5";
        professors[5] = "Professor 6";
        professors[6] = "Professor 7";
        professors[7] = "Professor 8";
        professors[8] = "Professor 9";
        professors[9] = "Professor 10";
        for(int a = 0; a < number_questions; a++){ // answers
            answers[a] = a + 1;
        }

    	Leader leader = new Leader(args[1],"/election","/leader", idx, professors[idx], answers, number_students, number_questions, q);
        try{
        	boolean success = leader.elect();
        	if (success) {
        		leader.compute();
        	} else {
        		while(true) {
        			//Waiting for a notification
        		}
            }         
        } catch (KeeperException e){
        	e.printStackTrace();
        } catch (InterruptedException e){
        	e.printStackTrace();
        }
    }

    public static void student(String args[], int number_students, Queue q) {
        Integer number_questions = new Integer(args[2]);
        Integer id = new Integer(args[4]);
        int answers[] = new int [number_questions];

        if (args[3].equals("s")) {
            Random rand = new Random();
            int max_solving = 60000; // maximum of 1 minute per question
            int answer_range = 5;

            for (int i = 0; i < number_questions; i++){
                try{
                    System.out.println("I'm thinking to answer question " + (i+1));
                    Thread.sleep(rand.nextInt(max_solving));
                    answers[i] = rand.nextInt(answer_range) + 1;
                    System.out.println("Student: " + id + " | Question: " + (i+1) + " | Solution: " + answers[i]);
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

            barrier("localhost", "/b1", number_students, id, 0);
            lock("localhost", 1000, id, answers, q); // 1 second to send answer
        }

        /*barrier("localhost", "/b2", number_students + 1, id, 1);

        if (args[3].equals("p")) {
            System.out.println("Professor");
            for(int a = 0; a < number_questions; a++){ // answers
                answers[a] = a + 1;
            }

            for (int i = 0; i < number_students * number_questions; i++) {
                try{
                    int r[] = q.consume();
                    System.out.println("Question: " + r[0] + " | Student: " + r[1]);
                } catch (KeeperException e){
                    i--;
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }*/
    }

    public static void lock(String host, long wait, int id, int answers[], Queue q) {
        Lock lock = new Lock(host,"/lock", wait, id, answers, q);
        try{
            boolean success = lock.lock();
            if (success) {
                lock.compute();
            } else {
                while(true) {
                    //Waiting for a notification
                }
            }
        } catch (KeeperException e){
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public static void barrier(String host, String root, int size, int id, int index) {
        Barrier b = new Barrier(host, root, new Integer(size));
        System.out.println("Waiting students to finish the essay (At barrier)");
        try{
            boolean flag = b.enter();
            if(!flag) System.out.println("Error when entering the barrier");
        } catch (KeeperException e){
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }

        try{
            b.leave();
        } catch (KeeperException e){

        } catch (InterruptedException e){

        }
        System.out.println("Going to return my essay (Going to lock)");
    }   
}
