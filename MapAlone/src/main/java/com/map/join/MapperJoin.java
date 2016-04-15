package com.map.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/4/15.
 */
public class MapperJoin {
    static class MapperJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        Map<String, String> map = new HashMap<String, String>();

        /**
         * 在调用map方法前会调用此方法，我们在此方法中导入我们本地的缓存文件。
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 获取字典文件内容 我的放在D盘下面
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\pdts.txt")));
            String s = "";
            while (!StringUtils.isBlank(s = br.readLine())) {
                String[] pdts = s.split(",");
                // 将内容封装到map中。
                map.put(pdts[0], pdts[1]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 这里面的key我们并没有用的到，可以不用理会
            // 例：1001	pd001	300  中间是制表符\t
            String res = value.toString();
            String[] fields = res.split("\t");
            String pinfo = map.get(fields[1]);
            String oid = fields[0];
            context.write(new Text(oid), new Text(fields[1] + "\t" + pinfo + "\t" + fields[2] + "\r\n"));
        }
    }

    /**
     * 这里只用到了mapper，并没有reduce的加入。
     *
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();

        job.setMapperClass(MapperJoinMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /**
         * 我们的orders文件放在D盘的input下面
         */
        FileInputFormat.setInputPaths(job, new Path("D:/input/"));
        FileSystem file = FileSystem.get(conf);
        Path outPath = new Path("D:/output");
        if (file.exists(outPath)) {
            // 如果目标下存在该文件夹，就将其删除。
            file.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        /**
         * 将产品列表缓存到task工作节点的工作目录中去
         */
        job.addCacheFile(new URI("file:///D:/pdts.txt"));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
