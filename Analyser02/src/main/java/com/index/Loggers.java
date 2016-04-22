package com.index;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2016/4/21.
 */
public class Loggers implements Writable {
    private String date;
    private String ip;
    private String session;
    private String url;
    private String referal;

    public Loggers() {

    }

    public void set(String date, String ip, String url, String referal, String session) {
        this.date = date;
        this.ip = ip;
        this.url = url;
        this.referal = referal;
        this.session = session;
    }

    public Loggers(String date, String ip, String url, String referal, String session) {
        this.date = date;
        this.ip = ip;
        this.url = url;
        this.referal = referal;
        this.session = session;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
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

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getReferal() {
        return referal;
    }

    public void setReferal(String referal) {
        this.referal = referal;
    }

    @Override
    public String toString() {
        return this.date + "\t" + this.getSession() + "\t" + this.getUrl() + "\t" + this.getReferal();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(date);
        out.writeUTF(ip);
        out.writeUTF(session);
        out.writeUTF(url);
        out.writeUTF(referal);
    }

    public void readFields(DataInput in) throws IOException {
        this.date = in.readUTF();
        this.ip = in.readUTF();
        this.session = in.readUTF();
        this.url = in.readUTF();
        this.referal = in.readUTF();
    }
}
