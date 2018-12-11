package com.jike.barrier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
        String barrierPath;
        private final String ourPath;
        String readyPath;
        private static final String READY_NODE = "ready";
        private final AtomicBoolean hasBeenNotified = new AtomicBoolean(false);
        private final Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                notifyFromWatcher();
            }
        };

        /**
         * Barrier constructor
         *
         * @param address
         * @param barrierPath
         * @param size
         */
        Barrier(String address, String barrierPath, int size) {
            super(address);
            this.barrierPath = barrierPath;
            this.size = size;
            this.ourPath = barrierPath + "/" + UUID.randomUUID().toString();
            this.readyPath = barrierPath + "/" + READY_NODE;

            // Create barrier node
            if (zk != null) {
                try {
                    Stat s = zk.exists(barrierPath, false);
                    if (s == null) {
                        zk.create(barrierPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("Keeper exception when instantiating Barrier: " + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
        }

        /**
         * Join barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
        boolean enter() throws Exception {
            boolean readyPathExists = zk.exists(readyPath, watcher) != null;

            zk.create(ourPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return readyPathExists || internalEnter();
        }

        private synchronized boolean internalEnter() throws Exception {
            boolean result = true;
            List<String> list = zk.getChildren(barrierPath, false);
            do {
                if (list.size() >= size) {
                    try {
                        zk.create(readyPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException ignore) {
                        // ignore
                    }
                    break;
                } else {
                    if (!hasBeenNotified.get()) {
                        wait();
                    }
                }
            } while (false);

            return result;
        }

        /**
         * Wait until all reach barrier
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
        synchronized boolean leave() throws Exception {
            boolean ourNodeShouldExist = true;
            boolean result = true;
            String ourPathName = getNodeFromPath(ourPath);

            while (true) {
                List<String> children = zk.getChildren(barrierPath, false);
                children = filterAndSortChildren(children);
                if (children == null || children.size() == 0) {
                    break;
                }

                int ourIndex = children.indexOf(ourPathName);

                if (ourIndex < 0 && ourNodeShouldExist) {
                    break;
                }

                if (children.size() == 1) {
                    if (ourNodeShouldExist && !children.get(0).equals(ourPathName)) {
                        throw new IllegalStateException(
                                String.format("Last path (%s) is not ours (%s)", children.get(0), ourPathName));
                    }
                    checkDeleteOurPath(ourNodeShouldExist);
                    break;
                }

                Stat stat;
                boolean isLowestNode = (ourIndex == 0);
                if (isLowestNode) {
                    String highestNodePath = barrierPath + "/" + children.get(children.size() - 1);
                    stat = zk.exists(highestNodePath, watcher);
                } else {
                    String lowestNodePath = barrierPath + "/" + children.get(0);
                    stat = zk.exists(lowestNodePath, watcher);
                    checkDeleteOurPath(ourNodeShouldExist);
                    ourNodeShouldExist = false;
                }

                if (stat != null) {
                    wait();
                }
            }

            try {
                zk.delete(readyPath, -1);
            } catch (KeeperException.NoNodeException ignore) {
                // ignore
            }
            return result;
        }

        private void checkDeleteOurPath(boolean shouldExist) throws Exception {
            if (shouldExist) {
                zk.delete(ourPath, 0);
            }
        }

        /**
         * sort the children node
         *
         * @param children
         * @return
         */
        private static List<String> filterAndSortChildren(List<String> children) {
            List<String> filterList = children.stream().filter(name -> !name.equals(READY_NODE)).collect(Collectors.toList());
            Collections.sort(filterList);
            return filterList;
        }

        /**
         * Given a full path, return the node name. i.e. "/one/two/three" will return
         * "three"
         *
         * @param path
         *            the path
         * @return the node
         */
        public static String getNodeFromPath(String path) {
            int i = path.lastIndexOf("/");
            if (i < 0) {
                return path;
            }
            if ((i + 1) >= path.length()) {
                return "";
            }
            return path.substring(i + 1);
        }

        private synchronized void notifyFromWatcher() {
            hasBeenNotified.set(true);
            notifyAll();
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
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out
                            .println("Keeper exception when instantiating queue: "
                                    + e.toString());
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

        boolean produce(int i) throws KeeperException, InterruptedException{
            ByteBuffer b = ByteBuffer.allocate(4);
            byte[] value;

            // Add child with value i
            b.putInt(i);
            value = b.array();
            zk.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);

            return true;
        }


        /**
         * Remove first element from the queue.
         *
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
        int consume() throws KeeperException, InterruptedException{
            int retvalue = -1;
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
                        String minNode = list.get(0);
                        for(String s : list){
                            Integer tempValue = new Integer(s.substring(7));
                            //System.out.println("Temporary value: " + tempValue);
                            if(tempValue < min) {
                                min = tempValue;
                                minNode = s;
                            }
                        }
                        System.out.println("Temporary value: " + root + "/" + minNode);
                        byte[] b = zk.getData(root + "/" + minNode,
                                false, stat);
                        zk.delete(root + "/" + minNode, 0);
                        ByteBuffer buffer = ByteBuffer.wrap(b);
                        retvalue = buffer.getInt();

                        return retvalue;
                    }
                }
            }
        }
    }

    public static void main(String args[]) {
        if (args[0].equals("qTest"))
            queueTest(args);
        else
            barrierTest(args);

    }

    public static void queueTest(String args[]) {
        Queue q = new Queue(args[1], "/app1");

        System.out.println("Input: " + args[1]);
        int i;
        Integer max = new Integer(args[2]);

        if (args[3].equals("p")) {
            System.out.println("Producer");
            for (i = 0; i < max; i++)
            try{
                q.produce(10 + i);
            } catch (KeeperException e){

            } catch (InterruptedException e){

            }
        } else {
            System.out.println("Consumer");

            for (i = 0; i < max; i++) {
                try{
                    int r = q.consume();
                    System.out.println("Item: " + r);
                } catch (KeeperException e){
                    i--;
                } catch (InterruptedException e){

                }
            }
        }
    }

    public static void barrierTest(String args[]) {
        Random rand = new Random();
        int QTY = rand.nextInt(1000);
        try {
            ExecutorService service = Executors.newFixedThreadPool(QTY);
            System.out.println("barrier size:" + QTY);
            for (int i = 0; i < QTY; ++i) {
                final Barrier barrier = new Barrier(args[1], "/b1", QTY);
                final int index = i;
                Callable<Void> task = () -> {
                    Thread.sleep((long) (3 * Math.random()));
                    System.out.println("Client #" + index + " enters");
                    barrier.enter();
                    System.out.println("Client #" + index + " begins processing");
                    Thread.sleep((long) (3000 * Math.random()));
                    barrier.leave();
                    System.out.println("Client #" + index + " left");
                    return null;
                };
                service.submit(task);
            }
            service.shutdown();
            service.awaitTermination(3, TimeUnit.MINUTES);
            System.out.println("Left barrier");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}