package com.infImpl;

import com.inf.UserLoginService;

/**
 * Created by Administrator on 2016/4/11.
 */
public class UserLoginServiceImpl implements UserLoginService {
    @Override
    public String login(String name, String password) {
        return "logged  in successfully";
    }
}
