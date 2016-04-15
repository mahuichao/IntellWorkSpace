package com.mhc.question02;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义Bean类，同之前所讲
 * Created by Administrator on 2016/4/14.
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private long upload;
    private long dnload;
    private long counter;

    public FlowBean() {
    }

    public FlowBean(long upload, long dnload) {
        this.upload = upload;
        this.dnload = dnload;
        this.counter = upload + dnload;
    }

    public void set(long upload, long dnload) {
        this.upload = upload;
        this.dnload = dnload;
        this.counter = upload + dnload;
    }


    public long getUpload() {
        return upload;
    }

    public void setUpload(long upload) {
        this.upload = upload;
    }

    public long getDnload() {
        return dnload;
    }

    public void setDnload(long dnload) {
        this.dnload = dnload;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upload);
        dataOutput.writeLong(dnload);
        dataOutput.writeLong(counter);
    }

    public void readFields(DataInput dataInput) throws IOException {
        upload = dataInput.readLong();
        dnload = dataInput.readLong();
        counter = dataInput.readLong();
    }

    @Override
    public String toString() {
        return upload + "\t" + dnload + "\t" + counter + "\r\n";
    }

    public int compareTo(FlowBean o) {
        return this.counter > o.getCounter() ? -1 : 1;
    }
}
