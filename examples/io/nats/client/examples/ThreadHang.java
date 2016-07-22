package io.nats.client.examples;

import io.nats.client.AsyncSubscription;
import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.client.MessageHandler;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ThreadHang implements Runnable {

    public ThreadHang() {
        try (Connection nc = new ConnectionFactory().createConnection()) {

            AsyncSubscription s = nc.subscribe("foo", new MessageHandler() {

                @Override
                public void onMessage(Message m) {
                    System.out.println("Received a message: " + new String(m.getData()));
                }
            });

            // s.close();
            s.unsubscribe();
            nc.close();
        } catch (IOException | TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

    }

    public static void main(String[] args) throws InterruptedException {

        Thread t = new Thread(new ThreadHang());
        t.start();
        t.join(5000);
        System.out.println("Exiting application");
        // System.exit(1);
    }
}
