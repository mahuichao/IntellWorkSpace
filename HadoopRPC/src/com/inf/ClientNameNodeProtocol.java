package com.inf;

/**
 * Created by Administrator on 2016/4/11.
 */
public interface ClientNameNodeProtocol {
    public static final long versionID = 1L;

    public String getMetaData(String path);
}
