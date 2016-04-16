package com.log.enhance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

/**
 * Created by Administrator on 2016/4/16.
 */
public class DataLoader {
    static Connection conn;
    static Statement stmt;
    static ResultSet rs;


    public static void getDataFromDB(HashMap<String, String> ruleMap) throws Exception {
        try {
            Class.forName("com.mysql.jdbc.Driver"); // 加载驱动
            conn = DriverManager.getConnection("jdbc:mysql://sun/test", "root", "hadoop");
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select http,content from url_rule");
            while (rs.next()) {
                ruleMap.put(rs.getString(1), rs.getString(2));
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

    }
}
