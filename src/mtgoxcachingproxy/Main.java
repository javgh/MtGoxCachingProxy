package mtgoxcachingproxy;

import java.io.IOException;
import java.net.ServerSocket;

public class Main {
    private static final long DEFAULT_ERROR_WAIT_TIME = 1 * 1000;
    private static final long MAXIMUM_ERROR_WAIT_TIME = 15 * 60 * 1000;

    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket server = new ServerSocket(10508);
        long current_error_wait_time = DEFAULT_ERROR_WAIT_TIME;

        while (true) {
            MtGoxCachingProxy proxy = new MtGoxCachingProxy(server);
            boolean hadSuccessfulRun = proxy.runProxy();

            if (hadSuccessfulRun) {
                current_error_wait_time = DEFAULT_ERROR_WAIT_TIME;
            } else {
                current_error_wait_time *= 2;
                if (current_error_wait_time > MAXIMUM_ERROR_WAIT_TIME)
                    current_error_wait_time = MAXIMUM_ERROR_WAIT_TIME;
            }
            Thread.sleep(current_error_wait_time);
        }
    }
}
