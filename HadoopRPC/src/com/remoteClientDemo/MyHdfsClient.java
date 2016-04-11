package com.remoteClientDemo;

import com.inf.ClientNameNodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by Administrator on 2016/4/11.
 */
public class MyHdfsClient {
    public static void main(String[] args) throws IOException {

        ClientNameNodeProtocol clientNameNodeProtocol = RPC.getProxy(ClientNameNodeProtocol.class, 1L, new InetSocketAddress("localhost", 8888), new Configuration());
        String data = clientNameNodeProtocol.getMetaData("/test");
        System.out.println(data);

    }
}
