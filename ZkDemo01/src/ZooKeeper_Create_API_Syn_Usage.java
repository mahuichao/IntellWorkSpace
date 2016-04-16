import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 本程序我使用了同步的节点创建接口
 * 在接口的使用中，我们分别创建了两种类型的节点
 *  临时节点和临时顺序节点，如果创建了临时节点，
 *  那么返回值就是当时传入的值，这里也就是test。
 *  如果创建了临时顺序节点，那么就会自动在节点
 *  后面加上一个数字后缀。（ 临时节点当程序完事后会
 *  被删除，永久节点会一直存在，除非人为删除）
 * Created by Administrator on 2016/4/12.
 */
public class ZooKeeper_Create_API_Syn_Usage implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZooKeeper zooKeeper = new ZooKeeper("192.168.152.1:2181", 5000, new ZooKeeper_Create_API_Syn_Usage());
        connectedSemaphore.await();

        String path1 = zooKeeper.create("/test", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Success create znode : " + path1);

        String path2 = zooKeeper.create("/test", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Success create znode: " + path2);
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("Recieve watched evnt: " + event);
        if (Event.KeeperState.SyncConnected == event.getState()) {
            connectedSemaphore.countDown();
        }
    }
}
