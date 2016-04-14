package com.mhc.flow;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2016/4/14.
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private static long upload; //  ��������
    private static long dnload; //  ��������
    private static long counter;  // ������

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

    public FlowBean() {


    }

    public static long getDnload() {
        return dnload;
    }

    public static void setDnload(long dnload) {
        FlowBean.dnload = dnload;
    }

    public static long getUpload() {
        return upload;
    }

    public static void setUpload(long upload) {
        FlowBean.upload = upload;
    }

    public static long getCounter() {
        return counter;
    }

    public static void setCounter(long counter) {
        FlowBean.counter = counter;
    }


    /**
     * ���л�����
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
     * �����л�����
     * ע�⣺�����л���˳�������л���ȫһ��
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
