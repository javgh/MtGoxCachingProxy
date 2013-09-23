package mtgoxcachingproxy;

import de.roderick.weberknecht.WebSocket;
import de.roderick.weberknecht.WebSocketEventHandler;
import de.roderick.weberknecht.WebSocketMessage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

public class MtGoxCachingProxy implements WebSocketEventHandler {
    private static final int CACHE_SIZE = 10000;
    private static final int SUCCESSFUL_RUN_SIZE = 1000;
    private static final int LONGEST_SILENT_TIME = 5 * 60 * 1000;

    private static final String MTGOX_URI = "wss://websocket.mtgox.com/mtgox";
    private static final String TRADE_CHANNEL_SUBSCRIBE_MSG
            = "{\"op\":\"subscribe\",\"channel\":\"dbf1dee9-4f2e-4a08-8cb7-748919a71b21\"}\n";
    private static final String TICKER_CHANNEL_SUBSCRIBE_MSG
            = "{\"op\":\"subscribe\",\"channel\":\"d5f06780-30a8-4a48-a2f8-7ed181b4a13f\"}\n";
    private static final String DEPTH_CHANNEL_SUBSCRIBE_MSG
            = "{\"op\":\"subscribe\",\"channel\":\"24e67e0d-1cad-4cc0-9e7a-f8523ef460fe\"}\n";

    private ServerSocket proxyServer;
    private URI mtGoxUri = null;
    private String origin = null;
    private Writer clientWriter = null;
    private WebSocket outgoingSocket = null;
    private boolean outgoingConnectionError = false;
    private boolean hadSuccessfulRun = false;
    private long timestampLastMessage = 0;
    private Queue<String> cache;
    private final Object cacheLock = new Object();

    public MtGoxCachingProxy(ServerSocket proxyServer, String origin) {
        this.cache = new ArrayDeque<String>(CACHE_SIZE);
        this.proxyServer = proxyServer;
        this.origin = origin;
        try {
            this.mtGoxUri = new URI(MTGOX_URI);
        } catch (URISyntaxException ex) { throw new RuntimeException(ex); }
    }

    public boolean runProxy() {
        System.out.println("Proxy ready");
        initOutgoingConnection();

        Socket client = null;
        while (!this.outgoingConnectionError) {
            try {
                // only handle a single client
                proxyServer.setSoTimeout(LONGEST_SILENT_TIME);
                boolean clientConnected = false;
                while (!clientConnected) {
                    try {
                        client = proxyServer.accept();
                        clientConnected = true;
                    } catch (SocketTimeoutException ex) {
                        /* use timeout to check for activity */
                        if (System.currentTimeMillis() - this.timestampLastMessage >
                            LONGEST_SILENT_TIME) {
                            System.out.println("No activity for a long time - starting over");
                            this.outgoingConnectionError = true;
                            break;
                        }
                    }
                }
                if (this.outgoingConnectionError)
                    break;

                System.out.println("New client connected to proxy");
                client.setSoTimeout(500); // don't block longer than 500 ms, to be able
                                          // do run some checks from time to time
                BufferedReader clientReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                this.clientWriter = new OutputStreamWriter(client.getOutputStream());

                // fabricate subscribe message
                // (they might get send twice, but that should not be a problem)
                this.clientWriter.write(TRADE_CHANNEL_SUBSCRIBE_MSG);
                this.clientWriter.write(TICKER_CHANNEL_SUBSCRIBE_MSG);
                this.clientWriter.write(DEPTH_CHANNEL_SUBSCRIBE_MSG);
                this.clientWriter.flush();

                // send contents of cache
                synchronized (this.cacheLock) {
                    for (String cacheEntry : this.cache) {
                        sendToClient(cacheEntry);
                    }
                }

                while (true) {
                    boolean timeoutOccured = false;
                    String line = null;

                    try {
                        line = clientReader.readLine();
                    } catch (SocketTimeoutException ex) { timeoutOccured = true; }

                    if (this.outgoingConnectionError) {
                        System.out.println("Lost connection to Mt.Gox - starting over");
                        break;
                    }
                    if (System.currentTimeMillis() - this.timestampLastMessage >
                            LONGEST_SILENT_TIME) {
                        System.out.println("No activity for a long time - starting over");
                        this.outgoingConnectionError = true;
                        break;
                    }
                    if (!timeoutOccured) {
                        if (line == null) {
                            System.out.println("Client disconnected");
                            break;
                        }
                        this.outgoingSocket.send(line);
                    }
                }
            } catch (IOException ex) {
                System.out.println("Client lost");
            } finally {
                // clean up client connection
                if (client != null) {
                    try {
                        client.close();
                    } catch (IOException ex) { /* can be ignored */ }
                }
            }
        }

        // clean up websocket connection
        closeOutgoingConnection();

        return this.hadSuccessfulRun;
    }

    private void initOutgoingConnection() {
        Map<String, String> extraHeaders = new TreeMap<String, String>();
        extraHeaders.put("Origin", origin);
        this.outgoingSocket = new WebSocket(this.mtGoxUri, null, extraHeaders);
        this.outgoingSocket.setEventHandler(this);

        System.out.println("Attempting outgoing connection");
        this.outgoingSocket.connect();
        this.timestampLastMessage = System.currentTimeMillis();
        this.hadSuccessfulRun = false;
    }

    private void closeOutgoingConnection() {
        if (this.outgoingSocket != null) { this.outgoingSocket.close(); }
    }

    private void sendToClient(String message) {
        String line = message + "\n";
        if (this.clientWriter != null) {
            try {
                this.clientWriter.write(line);
                this.clientWriter.flush();
            } 
            catch (IOException ex) { /* ignore if we couldn't send */ }
        }
    }

    public void onOpen() {
        synchronized (this.cacheLock) { this.cache.clear(); }
        System.out.println("Outgoing connection established");
    }

    public void onMessage(WebSocketMessage message) {
        this.timestampLastMessage = System.currentTimeMillis();
        synchronized (this.cacheLock) {
            if (this.cache.size() >= CACHE_SIZE) this.cache.remove();
            this.cache.add(message.getText());

            if (this.cache.size() >= SUCCESSFUL_RUN_SIZE)
                this.hadSuccessfulRun = true;
        }
        sendToClient(message.getText());
    }

    public void onClose() {
        this.outgoingConnectionError = true;
    }

    public void onError(IOException exception) {
        System.out.println("Websocket error: " + exception);
        this.outgoingConnectionError = true;
    }

    public void onPing() {
        /* do nothing */
    }

    public void onPong() {
        /* do nothing */
    }
}
