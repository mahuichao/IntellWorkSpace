package com.inf;

/**
 * Created by Administrator on 2016/4/11.
 */
public interface UserLoginService {
    public static final long versionID = 100L;

    public String login(String name, String password);
}
