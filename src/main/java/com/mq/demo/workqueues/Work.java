package com.mq.demo.workqueues;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by hzzhaolong on 2016/2/3.
 */
public class Work {

    private static final String QUEUE_NAME = "hzzhaolongtest";
    private static final int QUEUE_PORT = 5672;
    private static final String IP = "xxx";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "d97aNp";


    public static  void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(IP);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setPort(QUEUE_PORT);
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");

                System.out.println(" [x] Received '" + message + "'");
                try {
                    try {
                        doWork(message);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } finally {
                    System.out.println(" [x] Done");
                }
            }
        };
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
            // 模拟耗时任务
            if (ch == '.') Thread.sleep(1000);
        }
    }


}
