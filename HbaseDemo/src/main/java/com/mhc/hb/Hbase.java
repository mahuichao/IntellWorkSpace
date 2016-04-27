package com.mhc.hb;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/4/26.
 */
public class Hbase {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.152.4:2181,192.168.152.5:2181,192.168.152.6:2181");

        HBaseAdmin admin = new HBaseAdmin(conf);
        TableName name = TableName.valueOf("nvshen");
        HTableDescriptor desc = new HTableDescriptor(name);
        HColumnDescriptor base_info = new HColumnDescriptor("base_Info");
        HColumnDescriptor extra_info = new HColumnDescriptor("extra_Info");
        base_info.setMaxVersions(5);
        desc.addFamily(base_info);
        desc.addFamily(extra_info);
        admin.createTable(desc);
    }

    /**
     * 插入数据
     *
     * @throws IOException
     */
    @Test
    public void insertTest() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "moon:2181,jupiter:2181,neptune:2181");

        HTable nvshen = new HTable(conf, "nvshen");
        Put name = new Put(Bytes.toBytes("rk0001")); // 此处为rowkey
        name.add(Bytes.toBytes("base_Info"), Bytes.toBytes("name"), Bytes.toBytes("chaoge")); // base_info为列族 name为列族的列 最后chaoge为值

        Put age = new Put(Bytes.toBytes("rk0001"));
        age.add(Bytes.toBytes("base_Info"), Bytes.toBytes("age"), Bytes.toBytes(18));

        ArrayList<Put> puts = new ArrayList<Put>();
        puts.add(name);
        puts.add(age);

        nvshen.put(puts);
    }

    private Configuration conf = null;

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "moon,jupiter,neptune");
    }

    public void testDrop() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable("nb");
        admin.deleteTable("nb");
        admin.close();
    }


    @Test
    public void testGet() throws IOException {
        HTable table = new HTable(conf, "nvshen");
        Get get = new Get(Bytes.toBytes("rk0001"));
        get.setMaxVersions(5);
        Result result = table.get(get);
        List<Cell> cells = result.listCells();

        for (KeyValue kv : result.list()) {
            String family = new String(kv.getFamily());
            System.out.println(family);
            String qualifier = new String(kv.getQualifier());
            System.out.println(qualifier);
            System.out.println(new String(kv.getValue()));
        }
        table.close();
    }

}
