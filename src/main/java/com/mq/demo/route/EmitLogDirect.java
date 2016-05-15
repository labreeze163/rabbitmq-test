package com.mq.demo.route;

import com.mq.demo.workqueues.Send;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by hzzhaolong on 2016/2/3.
 */
public class EmitLogDirect {
    private static final String EXCHANGE_NAME = "direct_logs";
    private static final String QUEUE_NAME = "hzzhaolongtest";
    private static final int QUEUE_PORT = 5672;
    private static final String IP = "10.120.152.xxx";
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

        String severity = getSeverity(args);
        String message = Send.getMessage(args);

        // 主要关注这里的前两个参数一个是exchangeName 一个是routingKey
        channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes());
        System.out.println(" [x] Sent '" + severity + "':'" + message + "'");

        channel.close();
        connection.close();

    }

    private static String getSeverity(String[] strings) {
        if ((int)(Math.random()*10) > 5 )
            return "info";
        return "error";
    }


}
