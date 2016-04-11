import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

/**
 * 客户端去操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
 * <p/>
 * 也可以在构造客户端fs对象时，通过参数传递进去
 *
 * @author
 */
public class HdfsClientDemo {
    FileSystem fs = null;

    @Before
    public void init() throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.152.1:9000");

        //拿到一个文件系统操作的客户端实例对象
        /*fs = FileSystem.get(conf);*/
        //可以直接传入 uri和用户身份
        fs = FileSystem.get(new URI("hdfs://192.168.152.1:9000"), conf, "hadoop");
    }

    /**
     * 上传文件
     *
     * @throws Exception
     */
    public void testUpload() throws Exception {

        Thread.sleep(500000);
        fs.copyFromLocalFile(new Path("c:/access.log"), new Path("/access.log.copy"));
        fs.close();
    }


    /**
     * 从集群上下载文件
     *
     * @throws Exception
     */
    public void testDownload() throws Exception {

        fs.copyToLocalFile(new Path("/access.log.copy"), new Path("d:/"));
        fs.close();
    }

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://192.168.152.1:9000");
//        //拿到一个文件系统操作的客户端实例对象
//        FileSystem fs = FileSystem.get(conf);
//
//        fs.copyFromLocalFile(new Path("c:/access.log"), new Path("/access.log.copy"));
//        fs.close();
//    }

    /**
     * 在集群上删除文件
     *
     * @throws IOException
     */
    public void testDelete() throws IOException {
        fs.delete(new Path("/eclipse"), true);
    }

    /**
     * 在hadoop集群上创建文件夹
     *
     * @throws IOException
     */
    public void testAdd() throws IOException {
        fs.mkdirs(new Path("/aaa/bbbb/ccc"));
    }

    /**
     * 块的信息
     *
     * @throws IOException
     */
    @Test
    public void testlist() throws IOException {

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/test"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus filestatus = listFiles.next();
            System.out.println(filestatus.getBlockLocations());
            System.out.println(filestatus.getReplication());
            System.out.println(filestatus.getOwner());

            BlockLocation[] blockLocations = filestatus.getBlockLocations();
            for (BlockLocation b : blockLocations) {
                System.out.println("块长度" + b.getLength());
                System.out.println("块名称" + b.getHosts());
                System.out.println("块偏移" + b.getOffset());
            }
        }
    }

    /**
     * 查看根节点及下面的路径
     *
     * @throws IOException
     */
    @Test
    public void testLs2() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus file : listStatus) {
            System.out.println("name : " + file.getPath().getName());
            System.out.println(file.isFile() ? "file" : "directory");
        }
    }

}
