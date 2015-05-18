import java.io.*;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.Socket;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import com.umbrant.quantile.Quantile;
import com.umbrant.quantile.QuantileEstimationCKMS;

class Common {

    public static int benchmarkDuration;
    public static int tuplesPerSecond;
    public static int totalTuplesToSend;
    public static long startTime;
    public static String name;
    public static boolean started = false;
    public static final int SO_TIMEOUT = 60000;
    public static int totalSpout = 0;
    public static int deadBolt = 0;

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class DataGenerator {

    private ServerSocket socket;

    public DataGenerator () {

        new Thread() {
            @Override
            public void run () {
                while (Common.totalSpout == 0)
                    Common.sleep(1000);

                for (int countdown = 15; countdown > 0; countdown--) {
                    System.out.println("Starting in " + countdown + "...");
                    Common.sleep(1000);
                }
                Common.startTime = System.currentTimeMillis();
                Common.started = true;
                try { socket.close(); } catch (Exception e) { e.printStackTrace(); }
            }
        }.start();
    }

    class ConnectionHandler implements Runnable {
        private Socket socket;
        private int id;
        private int total;
        private int sent;
        private int rate;

        public ConnectionHandler(Socket socket) {
            this.socket = socket;
            this.id = Common.totalSpout++;
            System.out.println("New connection with spout " + this.id);
        }
        public void run() {
            while (!Common.started)
                Common.sleep(1000);

            this.total = Common.totalTuplesToSend / Common.totalSpout;
            this.rate = Common.tuplesPerSecond / Common.totalSpout;

            System.out.println("Generating " + this.total + " tuples for spout " + this.id + " at " + this.rate + " tup/s");

            PrintWriter out = null;
            try {
                out = new PrintWriter(socket.getOutputStream(), true);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 300; i++) { sb.append('a'); }
                String s = sb.toString();
                while(true) {
                    long intervalStart = System.currentTimeMillis();
                    for (int i = 0; i < this.rate; i++) {
                        out.println(System.currentTimeMillis());
                        //out.println(System.currentTimeMillis() + s);
                        out.flush();
                        sent++;
                    }
                    long extra = 1000 - (System.currentTimeMillis() - intervalStart);
                    //System.out.println("Sent " + Common.tuplesPerSecond + " tuples to " + id + " with extra time " + extra);
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
        finally { try { if (!socket.isClosed()) socket.close(); } catch (IOException e) { e.printStackTrace(); } }
    }
}

class DataListener {
    private ServerSocket socket;
    private int totalTuplesReceived = 0;
    private List<Quantile> quantiles;
    private QuantileEstimationCKMS estimator;
    private Object lock = new Object();

    public DataListener() {
        quantiles = new ArrayList<Quantile>();
        quantiles.add(new Quantile(0.50, 0.050));
        quantiles.add(new Quantile(0.90, 0.010));
        quantiles.add(new Quantile(0.95, 0.005));
        quantiles.add(new Quantile(0.99, 0.001));
        estimator = new QuantileEstimationCKMS(quantiles.toArray(new Quantile[] {}));

        new Thread() {
            public void run() {
                while (true) {
                    Common.sleep(10000);
                    if (Common.started)
                        System.out.printf("Received %d / %d tuples\n", totalTuplesReceived, Common.totalTuplesToSend);
                }
            }
        }.start();
    }

    class ConnectionHandler implements Runnable {
        Socket socket;
        int id;
        public ConnectionHandler(Socket socket) {
            this.socket = socket;
        }

        void printSummary(PrintWriter out, double avgBandwidth) throws IOException {
            out.println("===========================================================");
            out.println("Benchmark: " + Common.name + ", date: " + new Date().toString());
            out.printf("Finished benchmark. (%d tup/s), received %d/%d tuples\n", Common.tuplesPerSecond, totalTuplesReceived, Common.totalTuplesToSend);
            for (Quantile quantile : quantiles) {
                double q = quantile.quantile;
                long estimate = estimator.query(q);
                out.printf("Avg latency Q(%.2f, %.3f): %d ms, avg bandwidth: %f tup/s\n", quantile.quantile, quantile.error, estimate, avgBandwidth);
            }
            out.println("===========================================================");
            out.println();
        }

        void printSummary(boolean timedOut) throws IOException {
            synchronized (lock) {
                long endTime = System.currentTimeMillis();
                if (timedOut) endTime -= Common.SO_TIMEOUT;
                long totalTime = endTime - Common.startTime + 1000;
                double avgBandwidth = totalTuplesReceived / ((double) totalTime / 1000);
                PrintWriter out;

                out = new PrintWriter(System.out);
                printSummary(out, avgBandwidth);
                out.close();

                out = new PrintWriter(new BufferedWriter(new FileWriter("/root/results.txt", true)));
                printSummary(out, avgBandwidth);
                out.close();
            }
        }

        public void run() {
            BufferedReader in = null;
            boolean timedOut = false;
            try {
                this.socket.setSoTimeout(Common.SO_TIMEOUT);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msg;
                while ((msg = in.readLine()) != null) {
                    long receiveTime = System.currentTimeMillis();
                    long sendTime = Long.valueOf(msg.substring(0, 13));
                    long latency = receiveTime - sendTime;
                    synchronized (lock) {
                        totalTuplesReceived++;
                        estimator.insert(latency);
                    }
                    //if (totalTuplesReceived >= Common.totalTuplesToSend) {
                    //    break;
                    //}
                }
            }
            catch (SocketTimeoutException e) {
                System.out.println("Connection timed out");
                timedOut = true;
            }
            catch (Exception e) { e.printStackTrace(); }
            finally {
                try {
                    in.close();
                    socket.close();
                    synchronized (lock) {
                        Common.deadBolt++;
                    }
                    System.out.println("Closing: dead " + Common.deadBolt + " total " + Common.totalSpout);
                    synchronized (lock) {
                        if (Common.deadBolt >= Common.totalSpout) {
                            printSummary(timedOut);
                            System.exit(0);
                        }
                    }
                } catch (Exception e) { e.printStackTrace(); System.exit(0); }
            }
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


public class DataServer {
    public static void main(String[] args) {

        try {
            Common.tuplesPerSecond = Integer.parseInt(args[0]);
            Common.benchmarkDuration = Integer.parseInt(args[1]);
            Common.name = args[2];
        }
        catch (Exception e) {
            System.out.println("Usage: DataServer [tup/s] [duration] [name]");
            System.exit(1);
        }
        Common.totalTuplesToSend = Common.tuplesPerSecond * Common.benchmarkDuration;

        final DataGenerator dataGenerator = new DataGenerator();
        final DataListener dataListener = new DataListener();
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
    }
}
