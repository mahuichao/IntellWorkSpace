package com.mhc;

import com.mhc.com.mhc.DateUtils;
import com.mhc.com.mhc.bean.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/5/6.
 */
public class MonitorHandler {
    private static Logger logger = Logger.getLogger(MonitorHandler.class);
    //定义一个list，用来封装所有的应用信息
    private static List<App> applist;
    //定义一个list，用来封装所有的用户信息
    private static List<User> userList;
    //定义一个map，其中appId为Key，以该appId下的所有rule为Value
    private static Map<String, List<Rule>> ruleMap;
    //定义一个map,其中appId为Key，以该appId下的所有user为Value
    private static Map<String, List<User>> userMap;
    //定时加载配置文件的标识
    private static boolean reloaded = false;

    static {
        load();
    }

    /**
     * 加载数据模型，主要是用户列表、应用管理表、组合规则模型、组合用户模型。
     */
    public static synchronized void load() {
        if (userList == null) {
            userList = loadUserList();
        }
        if (applist == null) {
            applist = loadAppList();
        }
        if (ruleMap == null) {
            ruleMap = loadRuleMap();
        }
        if (userMap == null) {
            userMap = loadUserMap();
        }
    }

    /**
     * 访问数据库获取所有有效用户的列表
     *
     * @return
     */
    private static List<User> loadUserList() {
        return new LogMonitorDao().getUserList();
    }

    /**
     * 访问数据库获取所有有效的app列表
     *
     * @return
     */
    private static List<App> loadAppList() {
        return new LogMonitorDao().getAppList();
    }

    /**
     * 封装应用与用户对应的map
     *
     * @return
     */
    private static Map<String, List<User>> loadUserMap() {
        //以应用的appId为key，以应用的所有负责人的userList对象为value。
        //HashMap<String, List<User>>
        HashMap<String, List<User>> map = new HashMap<String, List<User>>();
        for (App app : applist) {
            String userIds = app.getUserId();
            List<User> userListInApp = map.get(app.getId());
            if (userListInApp == null) {
                userListInApp = new ArrayList<User>();
                map.put(app.getId() + "", userListInApp);
            }
            String[] userIdArr = userIds.split(",");
            for (String userId : userIdArr) {
                userListInApp.add(queryUserById(userId));
            }
            map.put(app.getId() + "", userListInApp);
        }
        return map;
    }


    /**
     * 通过用户编号获取用户的JavaBean
     *
     * @param userId
     * @return
     */
    private static User queryUserById(String userId) {
        for (User user : userList) {
            if (user.getId() == Integer.parseInt(userId)) {
                return user;
            }
        }
        return null;
    }

    /**
     * 封装应用与规则的map
     *
     * @return
     */
    private static Map<String, List<Rule>> loadRuleMap() {
        Map<String, List<Rule>> map = new HashMap<String, List<Rule>>();
        LogMonitorDao logMonitorDao = new LogMonitorDao();
        List<Rule> ruleList = logMonitorDao.getRuleList();
        //将代表rule的list转化成一个map，转化的逻辑是，
        // 从rule.getAppId作为map的key，然后将rule对象作为value传入map
        //Map<appId,ruleList>  一个appid的规则信息，保存在一个list中。
        for (Rule rule : ruleList) {
            List<Rule> ruleListByAppId = map.get(rule.getAppId() + "");
            if (ruleListByAppId == null) {
                ruleListByAppId = new ArrayList<Rule>();
                map.put(rule.getAppId() + "", ruleListByAppId);
            }
            ruleListByAppId.add(rule);
            map.put(rule.getAppId() + "", ruleListByAppId);
        }
        return map;
    }

    // 解析输入的日志
    public static Message parser(String line) {
        String[] messageArr = line.split("\\$\\$\\$\\$\\$");
        // 对日志进行排查
        if (messageArr.length != 2) {
            return null;
        }
        if (StringUtils.isBlank(messageArr[0]) || StringUtils.isBlank(messageArr[1])) {
            return null;
        }
        //检验当前日志所属的appid是否是经过授权的。
        if (apppIdisValid(messageArr[0].trim())) {
            Message message = new Message();
            message.setAppId(messageArr[0].trim());
            message.setLine(messageArr[1]);
            return message;
        }
        return null;
    }

    private static boolean apppIdisValid(String appId) {
        try {
            for (App app : applist) {
                if (app.getId() == Integer.parseInt(appId)) {
                    return true;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    /**
     * 对日志进行规制判定，看看是否触发规则
     *
     * @param message
     * @return
     */
    public static boolean trigger(Message message) {
        //如果规则模型为空，需要初始化加载规则模型
        if (ruleMap == null) {
            load();
        }
        //从规则模型中获取当前appid配置的规则
        System.out.println(message.getAppId());
        List<Rule> keywordByAppIdList = ruleMap.get(message.getAppId());
        for (Rule rule : keywordByAppIdList) {
            //如果日志中包含过滤过的关键词，即为匹配成功
            if (message.getLine().contains(rule.getKeyword())) {
                message.setRuleId(rule.getId() + "");
                message.setKeyword(rule.getKeyword());
                return true;
            }
        }
        return false;
    }

    /**
     * 配置scheduleLoad重新加载底层数据模型。
     */
    public static synchronized void reloadDataModel() {
        if (reloaded) {
            long start = System.currentTimeMillis();
            userList = loadUserList();
            applist = loadAppList();
            ruleMap = loadRuleMap();
            userMap = loadUserMap();
            reloaded = false;
            logger.info("配置文件reload完成，时间：" + DateUtils.getDateTime() + " 耗时：" + (System.currentTimeMillis() - start));
        }
    }

    /**
     * 定时加载配置信息
     * 配合reloadDataModel模块一起使用。
     * 主要实现原理如下：
     * 1，获取分钟的数据值，当分钟数据是10的倍数，就会触发reloadDataModel方法，简称reload时间。
     * 2，reloadDataModel方式是线程安全的，在当前worker中只有一个现成能够操作。
     * 3，为了保证当前线程操作完毕之后，其他线程不再重复操作，设置了一个标识符reloaded。
     * 在非reload时间段时，reloaded一直被置为true；
     * 在reload时间段时，第一个线程进入reloadDataModel后，加载完毕之后会将reloaded置为false。
     */
    public static void scheduleLoad() {
        String date = DateUtils.getDateTime();
        int now = Integer.parseInt(date.split(":")[1]);
        if (now % 10 == 0) {//每10分钟加载一次
            //1,2,3,4,5,6
            reloadDataModel();
        } else {
            reloaded = true;
        }
    }

    /**
     * 告警模块，用来提示人员错误信息（在真实生产中，可以在此处使用邮件或者短信业务通知）
     * 短信功能由于短信资源匮乏，目前默认返回已发送。
     *
     * @param appId
     * @param message
     */
    public static void notifly(String appId, Message message) {
        logger.info("-----------------------------------------------Hello I have something wrong with it");
    }

    /**
     * 保存触发规则的信息，将触发信息写入到mysql数据库中。
     *
     * @param record
     */
    public static void save(Record record) {
        new LogMonitorDao().saveRecord(record);
    }
}
