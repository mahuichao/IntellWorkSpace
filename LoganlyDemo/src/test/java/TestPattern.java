import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2016/4/15.
 */
public class TestPattern {
    public static void main(String[] args) {
        String s = "GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A0d9\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36";
        Pattern pattern = Pattern.compile("\\[(.*?)\\]\\s+\"(.*)\"$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(s);
        if (matcher.find()) {
            System.out.println(matcher.group(1) + "--> " + matcher.group(2));

        }
    }
}
