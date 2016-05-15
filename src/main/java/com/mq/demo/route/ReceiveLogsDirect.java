package com.mq.demo.route;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by hzzhaolong on 2016/2/3.
 */
public class ReceiveLogsDirect {

    private static final String EXCHANGE_NAME = "direct_logs";
    private static final int QUEUE_PORT = 5672;
    private static final String IP = "1xxx";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "d97aNp";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(IP);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setPort(QUEUE_PORT);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        // 生成系统指定名称的队列，方便回收
        String queueName = channel.queueDeclare().getQueue();

        /**
         * 这里启动多个Receive进行测试
         * 一个是args = new String[1];args[0] = "error";
         * 一个是args = new String[2];args[0] = "error";args[1] = "info";
         */
        args = new String[1];
        args[0] = "error";
        for(String severity : args){
            channel.queueBind(queueName, EXCHANGE_NAME, severity);
        }
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }
}
