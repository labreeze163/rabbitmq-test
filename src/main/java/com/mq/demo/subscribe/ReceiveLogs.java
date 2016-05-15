package com.mq.demo.subscribe;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by hzzhaolong on 2016/2/3.
 */
public class ReceiveLogs {
    private static final String EXCHANGE_NAME = "hzzhaolonglogs";
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
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        // 生成名字随机的队列，绑定exchange来接受信息
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }

}
