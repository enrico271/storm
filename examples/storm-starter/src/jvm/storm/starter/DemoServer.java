package storm.starter;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;


public class DemoServer {

    private static ServerSocket listener;

    public static void startServer() {

        System.out.println("Starting server");

        try {

            listener = new ServerSocket(2727);
            while (true)
                new Connection(listener.accept()).start();


        } catch (IOException e) { e.printStackTrace(); }
        finally {
            try {
                listener.close();
            } catch (IOException e) {}
        }

    }



    private static class Connection extends Thread {
        private Socket socket;

        public Connection(Socket socket) {
            System.out.println("New connection: " + socket.toString());
            this.socket = socket;
        }

        public void run() {

            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msg;
                while ((msg = in.readLine()) != null) {
                    System.out.println(msg);
                }
            }
            catch (IOException e) { e.printStackTrace(); }
            finally {
                System.out.println("Closing connection: " + socket.toString());
                try { socket.close(); } catch (IOException e) {e.printStackTrace();}
            }
        }
    }
}