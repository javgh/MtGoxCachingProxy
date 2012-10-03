package mtgoxcachingproxy;

import java.io.IOException;
import java.net.ServerSocket;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket server = new ServerSocket(10508);
        while (true) {
            MtGoxCachingProxy proxy = new MtGoxCachingProxy(server);
            proxy.start();
            proxy.join();
        }
    }
}
