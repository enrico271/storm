package storm.starter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

class DataGenerator {
    ServerSocket socket;
    static int idCounter = 0;
    int totalTuplesSent = 0;
    int totalTuplesToSend;
    int tuplesPerSecond;

    public DataGenerator (int totalTuplesToSend, int tuplesPerSecond) {
        this.totalTuplesToSend = totalTuplesToSend;
        this.tuplesPerSecond = tuplesPerSecond;
    }

    class ConnectionHandler implements Runnable {
        Socket socket;
        int id;
        int sent;
        int total;
        public ConnectionHandler(Socket socket) {
            this.socket = socket;
            this.id = idCounter++;
            this.sent = 0;
            this.total = totalTuplesToSend;
        }
        public void run() {
            PrintWriter out = null;
            try {
                out = new PrintWriter(socket.getOutputStream(), true);
                while(true) {
                    long intervalStart = System.currentTimeMillis();
                    for (int i = 0; i < tuplesPerSecond; i++) {
                        out.println(System.currentTimeMillis());
                        out.flush();
                        sent++;
                        synchronized(this) {
                            totalTuplesSent++;
                        }
                    }
                    long extra = 1000 - (System.currentTimeMillis() - intervalStart);
                    System.out.println("Sent " + tuplesPerSecond + " tuples to " + id + " with extra time " + extra);
                    if (out.checkError()) {
                        System.out.println("Disconnected " + id + " (or other error)");
                        break;
                    }
                    if (sent >= total) {
                        System.out.println("Finished sending " + sent + "/" + total + " tuples to " + id);
                        break;
                    }
                    if (extra > 0) {
                        Thread.sleep(extra);
                    }
                }
            }
            catch (Exception e) { e.printStackTrace(); }
            finally { try { out.close(); socket.close(); } catch (Exception e) { e.printStackTrace(); } }
        }
    }

    public void start(int port) {
        System.out.println("Starting generator on port " + port);
        try {
            socket = new ServerSocket(port);
            while (true)
                new Thread(new ConnectionHandler(socket.accept())).start();
        } catch (IOException e) { e.printStackTrace(); }
        finally { try { socket.close(); } catch (IOException e) { e.printStackTrace(); } }
    }
}

class DataListener {
    ServerSocket socket;
    static int idCounter = 0;
    int totalTuplesReceived = 0;
    long totalLatency = 0;
    int intervalTuplesReceived = 0;
    long intervalLatency = 0;

    class ConnectionHandler implements Runnable {
        Socket socket;
        int id;
        public ConnectionHandler(Socket socket) {
            this.socket = socket;
            this.id = idCounter++;
        }
        public void run() {
            BufferedReader in = null;
            try {
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msg;
                while ((msg = in.readLine()) != null) {
                    long receiveTime = System.currentTimeMillis();
                    long sendTime = Long.valueOf(msg);
                    long latency = receiveTime - sendTime;
                    synchronized(this) {
                        intervalTuplesReceived++;
                        intervalLatency += latency;
                    }
                }
            }
            catch (IOException e) { e.printStackTrace(); }
            finally { try { in.close(); socket.close(); } catch (Exception e) { e.printStackTrace(); } }
        }
    }

    public void start(int port) {
        System.out.println("Starting listener on port " + port);
        try {
            socket = new ServerSocket(port);
            while (true) {
                new Thread(new ConnectionHandler(socket.accept())).start();
            }
        } catch (IOException e) { e.printStackTrace(); }
        finally { try { socket.close(); } catch (IOException e) { e.printStackTrace(); } }
    }
}

class PrintStats implements Runnable {
    final DataGenerator dataGenerator;
    final DataListener dataListener;
    public PrintStats(DataGenerator dataGenerator, DataListener dataListener) {
        this.dataGenerator = dataGenerator;
        this.dataListener = dataListener;
    }
    public void run() {
        int lastTotalTuplesSent = 0;
        int lastTotalTuplesReceived = 0;
        while (true) {
            long curTime = System.currentTimeMillis();
            int intervalTuplesReceived;
            long intervalLatency;
            synchronized(dataListener) {
                intervalTuplesReceived = dataListener.intervalTuplesReceived;
                intervalLatency = dataListener.intervalLatency;
                dataListener.intervalTuplesReceived = 0;
                dataListener.intervalLatency = 0;
            }
            dataListener.totalTuplesReceived += intervalTuplesReceived;
            dataListener.totalLatency += intervalLatency;
            if (dataGenerator.totalTuplesSent > lastTotalTuplesSent
                    || dataListener.totalTuplesReceived > lastTotalTuplesReceived) {
                System.out.print(System.currentTimeMillis());
                System.out.print("    Interval received: " + intervalTuplesReceived);
                System.out.print(" latency: " + ((double) intervalLatency / intervalTuplesReceived));
                System.out.print("    Total received: " + dataListener.totalTuplesReceived);
                System.out.print(" latency: " + ((double) dataListener.totalLatency / dataListener.totalTuplesReceived));
                System.out.print("    Total sent: " + dataGenerator.totalTuplesSent);
                System.out.println();
            }
            lastTotalTuplesSent = dataGenerator.totalTuplesSent;
            lastTotalTuplesReceived = dataListener.totalTuplesReceived;
            try {
                Thread.sleep(1000 - (System.currentTimeMillis() - curTime));
            } catch (InterruptedException e) { e.printStackTrace(); }
        }
    }
}

public class DataServer {
    public static void main(String[] args) {
        int totalTuplesToSend = Integer.valueOf(args[0]);
        int tuplesPerSend = Integer.valueOf(args[1]);
        final DataGenerator dataGenerator = new DataGenerator(totalTuplesToSend, tuplesPerSend);
        final DataListener dataListener = new DataListener();
        final PrintStats printStats = new PrintStats(dataGenerator, dataListener);
        new Thread() {
            @Override
            public void run() {
                dataGenerator.start(2222);
            }
        }.start();
        new Thread() {
            @Override
            public void run() {
                dataListener.start(3333);
            }
        }.start();
        new Thread(printStats).start();
    }
}
