import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 本程序中可以看出，使用异步方式也很简单。仅仅实现AsyncCallback.StringCallback
 * 接口即可。同时仅仅实现AsyncCallback也包含很多其他接口，可以实现不同的逻辑
 * 合同步的区别在于，节点的创建过程是异步的，并且，在同步接口的调用中，我们需要
 * 关注接口跑出异常的可能。但是在异步的几口中，接口本身是不会抛出异常的，所有的异常
 * 都会在回调函数中通过ResultCode来体现。
 * <p/>
 * Created by Administrator on 2016/4/16.
 */
public class ZooKeeper_Create_API_ASync_Usage implements Watcher {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("sun:2181", 5000, new ZooKeeper_Create_API_ASync_Usage());
        connectedSemaphore.await();
        zk.create("/test02", "hello everybody".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, new IStringCallback(), "I am context");
        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent event) {
        if (Event.KeeperState.SyncConnected == event.getState()) {
            connectedSemaphore.countDown();
        }
    }

    static class IStringCallback implements AsyncCallback.StringCallback {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            System.out.println("create path result:[" + rc + "," + path + "," + ctx + ", real path name:" + name);
        }
    }
}
