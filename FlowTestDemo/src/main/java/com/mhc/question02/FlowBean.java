package com.mhc.question02;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2016/4/14.
 */
public class FlowBean  implements WritableComparable<FlowBean> {
    private  long upload; //  上行流量
    private  long dnload; //  下行流量
    private  long counter;  // 计数器
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

    /**
     * 序列化方法
     *
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upload);
        dataOutput.writeLong(dnload);
        dataOutput.writeLong(counter);
    }

    /**
     * 反序列化方法
     * 注意：反序列化的顺序与序列化完全一致
     *
     * @param dataInput
     * @throws IOException
     */
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
