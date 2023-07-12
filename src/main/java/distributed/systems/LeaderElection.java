package distributed.systems;

import org.apache.zookeeper.*;

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
        leaderElection.electLeader();

        leaderElection.run();
        leaderElection.close();

        LOGGER.info("Disconnected from Zookeeper, exiting application");
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {

        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        LOGGER.info("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void electLeader() throws InterruptedException, KeeperException {

        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);

        String smallestChildren = children.get(0);

        if (smallestChildren.equals(currentZnodeName)) {
            LOGGER.info("I am the leader");
            return;
        }
        LOGGER.info("I am note the leader, " + smallestChildren + " is the leader");
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
        }
    }
}
