package com.mq.demo.workqueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Created by hzzhaolong on 2016/2/3.
 */
public class Send {

    private static final String QUEUE_NAME = "hzzhaolongtest";
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

        // 第二个参数标记是否持久化队列
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = getMessage(args);

        channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }

    /**
     * 生产消息
     * @param strings
     * @return
     */
    public static String getMessage(String[] strings){
        /**
         * 随机数用于测试同一条消息只被一个消费者处理还是多个消费者同时处理
         * 测试证明，如果是这种策略的话只会被一个消费者接受和处理
         */
        if (strings.length < 1)
            return "Hello World!..." + (int)(Math.random()*100);
        return joinStrings(strings, " ");
    }

    public static String joinStrings(String[] strings, String delimiter) {
        int length = strings.length;
        if (length == 0) return "";
        StringBuilder words = new StringBuilder(strings[0]);
        for (int i = 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }

}
