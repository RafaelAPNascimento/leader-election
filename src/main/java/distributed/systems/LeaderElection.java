package distributed.systems;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public class LeaderElection implements Watcher {

    private static final Logger LOGGER = Logger.getLogger(LeaderElection.class.getName());

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";

    private String currentZnodeName;

    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        LeaderElection leaderElection = new LeaderElection();

        leaderElection.connectToZookeeper();

        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();

        leaderElection.run();
        leaderElection.close();

        LOGGER.info("This znode just disconnected from Zookeeper, exiting application");
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {

        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        LOGGER.info("this znode name is " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reelectLeader() throws InterruptedException, KeeperException {

        Stat predecessorStat = null;

        String predecessorZNodeName = null;

        while (predecessorStat == null) {

            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);

            String smallestChildren = children.get(0);

            if (smallestChildren.equals(currentZnodeName)) {
                LOGGER.info("I am the leader");
                return;
            }
            else {
                LOGGER.info("I am not the leader, " + smallestChildren + " is the leader");
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZNodeName = children.get(predecessorIndex);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZNodeName, this);
            }
        }
        LOGGER.info("Watching znode " + predecessorZNodeName);
    }

    private void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    private void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    @Override
    public void process(WatchedEvent event) {

        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    LOGGER.info("Successfully connected to Zookeeper!");
                }
                else {
                    synchronized (zooKeeper) {
                        zooKeeper.notifyAll();
                        LOGGER.info("Disconnected from Zookeeper event");
                    }
                }
                break;
            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (KeeperException | InterruptedException e) {}
        }
    }
}
