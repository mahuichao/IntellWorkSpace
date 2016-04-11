package com.infImpl;

import com.inf.ClientNameNodeProtocol;

/**
 * Created by Administrator on 2016/4/11.
 */
public class MyNameNode implements ClientNameNodeProtocol {

    @Override
    public String getMetaData(String path) {
        return path+": 3 -{ BLK_1,BLK_2 }";
    }
}
