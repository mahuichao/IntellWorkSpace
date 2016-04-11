import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Administrator on 2016/4/11.
 */
public class HdfsStreamAccess {
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.152.1:9000");

        //�õ�һ���ļ�ϵͳ�����Ŀͻ���ʵ������
        /*fs = FileSystem.get(conf);*/
        //����ֱ�Ӵ��� uri���û����
        fs = FileSystem.get(new URI("hdfs://192.168.152.1:9000"), conf, "hadoop");
    }

    /**
     * ����Ӧ�ļ��е����ݶ�ȡ����Ӧ�ڵ���
     *
     * @throws IOException
     */
    public void testUpload() throws IOException {
        FSDataOutputStream outputStream = fs.create(new Path("/test"));
        FileInputStream inputStream = new FileInputStream("D:/hadoop-2.6.1.zip");
        IOUtils.copy(inputStream, outputStream);
        System.out.println("�ϴ����");
    }


    /**
     * ����Ӧ�ڵ��µ����ݶ�ȡ������xx�ļ���
     *
     * @throws IOException
     */
    public void download() throws IOException {

        FSDataInputStream inputStream = fs.open(new Path("/test"));
        FileOutputStream outputStream = new FileOutputStream("D:/text.txt");
        IOUtils.copy(inputStream, outputStream);
        System.out.println("�������");
    }

    /**
     * ��ȡconf�еĸ���Ĭ������
     */
    public void testConf() {
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> ent = iterator.next();
            System.out.println(ent.getKey() + ":" + ent.getValue());
        }
    }

    /**
     * �ӵڼ����ֽڿ�ʼ��ȡ
     */
    public void testRandomAccess() throws IOException {

        FSDataInputStream inputStream = fs.open(new Path("/test"));

        inputStream.seek(12);
        FileOutputStream outputStream = new FileOutputStream("D:/12.xls");
        IOUtils.copyLarge(inputStream, outputStream);
    }

}
