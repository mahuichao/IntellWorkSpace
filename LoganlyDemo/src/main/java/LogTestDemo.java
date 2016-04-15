import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 我遇到这个问题第一反应是正则表达式来处理，如果不会的朋友也不要紧（一步步切分字符串，耐心点）
 * //                            _ooOoo_
 * //                           o8888888o
 * //                           88" . "88
 * //                           (| -_- |)
 * //                            O\ = /O
 * //                        ____/`---'\____
 * //                      .   ' \\| |// `.
 * //                       / \\||| : |||// \
 * //                     / _||||| -:- |||||- \
 * //                       | | \\\ - /// | |
 * //                     | \_| ''\---/'' | |
 * //                      \ .-\__ `-` ___/-. /
 * //                   ___`. .' /--.--\ `. . __
 * //                ."" '< `.___\_<|>_/___.' >'"".
 * //               | | : `- \`.;`\ _ /`;.`/ - ` : | |
 * //                 \ \ `-. \_ __\ /__ _/ .-` / /
 * //         ======`-.____`-.___\_____/___.-`____.-'======
 * //                            `=---='
 * Created by Administrator on 2016/4/15.
 */
public class LogTestDemo {
    /**
     * 公用方法，匹配字符串使用
     *
     * @param regex
     * @param line
     * @return
     */
    public static Matcher getMatcher(String regex, String line) {
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(line);
        return matcher;
    }

    static class logTestDemoMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = getMatcher("(.*)\\s{1}-\\s{1}-\\s{1}(.*)", line);
            /**
             *    将字段分为了两部分
             *    例子：【194.237.142.21,[18/Sep/2013:06:49:18 +0000] "GET /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"】
             */
            String ip = ""; // 暂且让寡人把他当做IP来使用
            String info = ""; // ip后边的部分
            if (matcher.find()) {
                ip = matcher.group(1); // ip地址
                info = matcher.group(2);
                context.write(new Text(ip), new Text(info));
            }
        }
    }

    static class logTestDemoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer result = new StringBuffer();
            for (Text text : values) {
                String res = text.toString();
                Matcher matcher1 = getMatcher("\\[(.*?)\\]\\s+\"(.*)\"$", res);
                String date = "";
                String otinfo = "";
                if (matcher1.find()) {
                    date = matcher1.group(1); // 日期字段
                    otinfo = matcher1.group(2); // 其他字段
                }


                /**
                 * 其实这里可以把这些属性封装成一个类，然后
                 * 传递的时候传递类(我懒)
                 * 日期类型处理
                 */
                String[] dates = date.split("/");
                String day = dates[0];
                String month = dates[1];
                String year = dates[2].split(":")[0];
                String hour = dates[2].split(":")[1];
                String min = dates[2].split(":")[2];
                String sec = dates[2].split(":")[3].split(" ")[0];
                String inValid = "";
                String acceptByte = "";
                String httpUlr = "";
                String browser = "";
                /**
                 * 网站类型处理
                 */
                String s = otinfo;
                Matcher matcher2 = getMatcher("(.*?)\"\\s+(.*?)\\s*\"(.*?)\"\\s+\"(.*?)", s);
                if (matcher2.find()) {
                    inValid = matcher2.group(1);
                    acceptByte = matcher2.group(2);
                    httpUlr = matcher2.group(3);
                    browser = matcher2.group(4);
                }
                result.append("--->").append(year + "-" + month + "-" + day + " " + hour + ":" + min + ":" + sec)
                        .append("--->").append(inValid).append("--->").append(acceptByte).append("--->").append(httpUlr)
                        .append("--->").append(browser);
            }
            context.write(new Text(key), new Text(result.toString() + "\r\n"));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(logTestDemoMapper.class);
        job.setReducerClass(logTestDemoReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job, new Path("file:///D:/access.log.fensi")); // 我把我的文件放在了这个下面
        Path outPath = new Path("file:///D:/output"); // 输出路径在这下面
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        boolean bl = job.waitForCompletion(true);
        System.exit(bl ? 0 : 1);
    }

}
