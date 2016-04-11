package com.remoteClientDemo;

import com.inf.UserLoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by Administrator on 2016/4/11.
 */
public class UserLoginAction {
    public static void main(String[] args) throws IOException {
        UserLoginService userLoginService = RPC.getProxy(UserLoginService.class, 1L, new InetSocketAddress("localhost", 9999), new Configuration());
        String login = userLoginService.login("good", "1234");
        System.out.println(login);
    }
}
