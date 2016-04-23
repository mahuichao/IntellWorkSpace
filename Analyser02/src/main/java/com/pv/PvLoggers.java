package com.pv;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2016/4/23.
 */
public class PvLoggers implements Writable {
    private String ip;
    private String session;
    private String date;
    private String url;
    private String stayTime;
    private String step;

    public PvLoggers() {
    }

    public PvLoggers(String ip, String session, String date, String url, String stayTime, String step) {
        this.ip = ip;
        this.session = session;
        this.date = date;
        this.url = url;
        this.stayTime = stayTime;
        this.step = step;
    }

    public void set(String ip, String session, String date, String url, String stayTime, String step) {
        this.ip = ip;
        this.session = session;
        this.date = date;
        this.url = url;
        this.stayTime = stayTime;
        this.step = step;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getStayTime() {
        return stayTime;
    }

    public void setStayTime(String stayTime) {
        this.stayTime = stayTime;
    }

    public String getStep() {
        return step;
    }

    public void setStep(String step) {
        this.step = step;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(ip);
        out.writeUTF(session);
        out.writeUTF(date);
        out.writeUTF(url);
        out.writeUTF(stayTime);
        out.writeUTF(step);
    }

    public void readFields(DataInput in) throws IOException {
        this.ip = in.readUTF();
        this.session = in.readUTF();
        this.date = in.readUTF();
        this.url = in.readUTF();
        this.stayTime = in.readUTF();
        this.step = in.readUTF();
    }

    @Override
    public String toString() {
        return this.date + "\t" + this.session + "\t" + this.url + "\t" + this.stayTime + "\t" + this.step;
    }
}
