import java.util.Random;

/**
 * Created by Administrator on 2016/4/27.
 */
public class MyTest {
    public static void main(String[] args) {
//        for (int i = 0; i < 10; i++) {
//            System.out.println((int) (1 + Math.random() * 10));
//        }
        for (int i = 0; i < 100; i++) {
            Random random = new Random();
            System.out.println(random.nextInt(10));
        }
    }
}
