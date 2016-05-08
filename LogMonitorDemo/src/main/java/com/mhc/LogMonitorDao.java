package com.mhc;

import com.mhc.com.mhc.bean.App;
import com.mhc.com.mhc.bean.Record;
import com.mhc.com.mhc.bean.Rule;
import com.mhc.com.mhc.bean.User;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Date;
import java.util.List;

/**
 * Created by Administrator on 2016/5/6.
 */
public class LogMonitorDao {
    private JdbcTemplate jdbcTemplate;

    public LogMonitorDao() {
        jdbcTemplate = new JdbcTemplate(DataSourceUtil.getDataSource());
    }

    /**
     * 查询所有用户的信息
     *
     * @return
     */
    public List<User> getUserList() {
        String sql = "SELECT `id`,`name`,`mobile`,`email`,`isValid` FROM `log_monitor`.`log_monitor_user` WHERE isValid =1";
        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<User>(User.class));
    }

    /**
     * 查询所有应用的信息
     *
     * @return
     */
    public List<App> getAppList() {
        String sql = "SELECT `id`,`name`,`isOnline`,`typeId`,`userId`  FROM `log_monitor`.`log_monitor_app` WHERE isOnline =1";
        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<App>(App.class));
    }

    /**
     * 查询所有规则信息
     *
     * @return
     */
    public List<Rule> getRuleList() {
        String sql = "SELECT `id`,`name`,`keyword`,`isValid`,`appId` FROM `log_monitor`.`log_monitor_rule` WHERE isValid =1";
        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<Rule>(Rule.class));
    }

    /**
     * 插入触发规则的信息
     *
     * @param record
     */
    public void saveRecord(Record record) {
        String sql = "INSERT INTO `log_monitor`.`log_monitor_rule_record`" +
                " (`appId`,`ruleId`,`isEmail`,`isPhone`,`isColse`,`noticeInfo`,`updataDate`) " +
                "VALUES ( ?,?,?,?,?,?,?)";
        jdbcTemplate.update(sql, record.getAppId(), record.getRuleId(), record.getIsEmail(), record.getIsPhone(), 0, record.getLine(), new Date());
    }
}
