import com.inf.ClientNameNodeProtocol;
import com.inf.UserLoginService;
import com.infImpl.MyNameNode;
import com.infImpl.UserLoginServiceImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/11.
 */
public class PublishServiceUtil {
    public static void main(String[] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("localhost").setPort(8888).setProtocol(ClientNameNodeProtocol.class).setInstance(new MyNameNode());
        RPC.Server server = builder.build();
        server.start();


        RPC.Builder builder2 = new RPC.Builder(new Configuration());
        builder2.setBindAddress("localhost").setPort(9999).setProtocol(UserLoginService.class).setInstance(new UserLoginServiceImpl());
        RPC.Server server2 = builder2.build();
        server2.start();
    }
}
