package com.mq.demo.subscribe;

import com.mq.demo.workqueues.Send;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by hzzhaolong on 2016/2/3.
 */
public class EmitLog {

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

        // Declare exchange
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        String message = getMessage(args);

        channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }

    private static String getMessage(String[] strings){
        if (strings.length < 1)
            return "Hello World!..." + (int)(Math.random()*100);;
        return Send.joinStrings(strings, " ");
    }


}
