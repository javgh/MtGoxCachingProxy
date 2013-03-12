package mtgoxcachingproxy;

import io.socket.IOAcknowledge;
import io.socket.IOCallback;
import io.socket.SocketIO;
import io.socket.SocketIOException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Queue;
import org.json.JSONException;
import org.json.JSONObject;

public class MtGoxCachingProxy implements IOCallback {
    private static final int CACHE_SIZE = 10000;
    private static final int SUCCESSFUL_RUN_SIZE = 1000;
    private static final int LONGEST_SILENT_TIME = 5 * 60 * 1000;

    private ServerSocket proxyServer;
    private URL mtGoxUrl = null;
    private Writer clientWriter = null;
    private SocketIO outgoingSocket = null;
    private boolean outgoingConnectionError = false;
    private boolean hadSuccessfulRun = false;
    private long timestampLastMessage = 0;
    private Queue<JSONObject> cache;
    private final Object cacheLock = new Object();

    public MtGoxCachingProxy(ServerSocket proxyServer) {
        this.cache = new ArrayDeque<JSONObject>(CACHE_SIZE);
        this.proxyServer = proxyServer;
        try {
            this.mtGoxUrl = new URL("https://socketio.mtgox.com/mtgox");
        } catch (MalformedURLException ex) { throw new RuntimeException(ex); }
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
                this.clientWriter.write("{\"op\":\"subscribe\",\"channel\":\"dbf1dee9-4f2e-4a08-8cb7-748919a71b21\"}\n");
                this.clientWriter.write("{\"op\":\"subscribe\",\"channel\":\"d5f06780-30a8-4a48-a2f8-7ed181b4a13f\"}\n");
                this.clientWriter.write("{\"op\":\"subscribe\",\"channel\":\"24e67e0d-1cad-4cc0-9e7a-f8523ef460fe\"}\n");
                this.clientWriter.flush();

                // send contents of cache
                synchronized (this.cacheLock) {
                    for (JSONObject cacheEntry : this.cache) {
                        sendJSON(cacheEntry);
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

                        try {
                            this.outgoingSocket.send(new JSONObject(line));
                        } catch (JSONException ex) { throw new RuntimeException(ex); }
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
        if (this.outgoingSocket != null) {
            this.outgoingSocket.disconnect();
        }

        return this.hadSuccessfulRun;
    }

    private void initOutgoingConnection() {
        System.out.println("Attempting outgoing connection");
        this.outgoingSocket = new SocketIO(this.mtGoxUrl);
        this.outgoingSocket.connect(this);
        this.timestampLastMessage = System.currentTimeMillis();
        this.hadSuccessfulRun = false;
    }

    private void closeOutgoingConnection() {
        if (this.outgoingSocket != null) { this.outgoingSocket.disconnect(); }
    }

    private void sendJSON(JSONObject jsono) {
        String line = jsono.toString() + "\n";
        if (this.clientWriter != null) {
            try {
                this.clientWriter.write(line);
                this.clientWriter.flush();
            } 
            catch (IOException ex) { /* ignore if we couldn't send */ }
        }
    }

    public void onConnect() {
        synchronized (this.cacheLock) { this.cache.clear(); }
        System.out.println("Outgoing connection established");
    }

    public void onMessage(JSONObject jsono, IOAcknowledge ioa) {
        this.timestampLastMessage = System.currentTimeMillis();
        synchronized (this.cacheLock) {
            if (this.cache.size() >= CACHE_SIZE) this.cache.remove();
            this.cache.add(jsono);

            if (this.cache.size() >= SUCCESSFUL_RUN_SIZE)
                this.hadSuccessfulRun = true;
        }
        sendJSON(jsono);
    }

    public void onDisconnect() {
        this.outgoingConnectionError = true;
    }

    public void onError(SocketIOException sioe) {
        System.out.println("Websocket error: " + sioe);
        this.outgoingConnectionError = true;
    }


    public void onMessage(String string, IOAcknowledge ioa) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void on(String string, IOAcknowledge ioa, Object... os) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
