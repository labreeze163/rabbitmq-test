package com.mq.demo.rpc;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;
import java.util.UUID;
/**
 *  Created by hzzhaolong on 2016/2/3.
 */

public class RPCClient {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "hzzhaolong_rpc_queue";
    private String replyQueueName;
    private QueueingConsumer consumer;
    private static final int QUEUE_PORT = 5672;
    private static final String IP = "xxx";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "d97aNp";

    public RPCClient() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(IP);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setPort(QUEUE_PORT);
        connection = factory.newConnection();
        channel = connection.createChannel();

        // 请求相应的消息队列
        replyQueueName = channel.queueDeclare().getQueue();
        consumer = new QueueingConsumer(channel);
        channel.basicConsume(replyQueueName, true, consumer);
    }

    public String call(String message) throws Exception {
        String response = null;
        String corrId = UUID.randomUUID().toString();

        // 封装请求的属性
        BasicProperties props = new BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            // 判断和自己的请求ID是否相同
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response = new String(delivery.getBody(),"UTF-8");
                break;
            }
        }
        return response;
    }

    public void close() throws Exception {
        connection.close();
    }

    public static void main(String[] argv) {
        RPCClient fibonacciRpc = null;
        String response = null;
        try {
            fibonacciRpc = new RPCClient();

            System.out.println(" [x] Requesting fib(30)");
            response = fibonacciRpc.call("30");
            System.out.println(" [.] Got '" + response + "'");
        }
        catch  (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (fibonacciRpc!= null) {
                try {
                    fibonacciRpc.close();
                }
                catch (Exception ignore) {}
            }
        }
    }
}