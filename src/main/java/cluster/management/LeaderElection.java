package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public class LeaderElection implements Watcher {

    private static final Logger LOGGER = Logger.getLogger(LeaderElection.class.getName());

    private static final String ELECTION_NAMESPACE = "/election";

    private String currentZnodeName;

    private ZooKeeper zooKeeper;

    public LeaderElection(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
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

    @Override
    public void process(WatchedEvent event) {

        switch (event.getType()) {
            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (KeeperException | InterruptedException e) {}
        }
    }
}
