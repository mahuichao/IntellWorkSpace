package com.log.enhance;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/16.
 */
public class LogEnhanceOutputFormat extends FileOutputFormat<Text, NullWritable> {


    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        Path enhancePath = new Path("D:/log.dat");
        Path toScrawlPath = new Path("D:/url.dat");

        FSDataOutputStream enos = fileSystem.create(enhancePath);
        FSDataOutputStream out = fileSystem.create(toScrawlPath);

        return new MyRecordWriter(enos, out);
    }

    static class MyRecordWriter extends RecordWriter<Text, NullWritable> {
        FSDataOutputStream enos = null;
        FSDataOutputStream out = null;

        public MyRecordWriter(FSDataOutputStream enos, FSDataOutputStream out) {
            super();
            this.enos = enos;
            this.out = out;
        }

        @Override
        public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
            if (text.toString().contains("toCrawl")) {
                out.write(text.toString().getBytes());  // 如果是源库总不存在的，写入此目录
            } else {
                enos.write(text.toString().getBytes()); // 如果写出的是增强的日志，写入此目录
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (enos != null) {
                enos.close();
            }
            if (out != null) {
                out.close();
            }
        }
    }
}

