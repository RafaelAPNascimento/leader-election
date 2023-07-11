package distributed.systems;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;

public class LeaderElection implements Watcher {

    private static final Logger LOGGER = Logger.getLogger(LeaderElection.class.getName());

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";

    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException {

        LeaderElection leaderElection = new LeaderElection();

        leaderElection.connectToZookeeper();
        leaderElection.run();
        leaderElection.close();

        LOGGER.info("Disconnected from Zookeeper, exiting application");
    }

    public void volunteerForLeadership() {

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
