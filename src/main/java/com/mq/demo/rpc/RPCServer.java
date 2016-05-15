package com.mq.demo.rpc;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;


/**
 * Created by hzzhaolong on 2016/2/3.
 */
public class RPCServer {

    private static final String RPC_REQUEST_QUEUE_NAME = "hzzhaolong_rpc_queue";
    private static final int QUEUE_PORT = 5672;
    private static final String IP = "xxx";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "d97aNp";

    private static int fib(int n) {
        if (n ==0) return 0;
        if (n == 1) return 1;
        return fib(n-1) + fib(n-2);
    }

    public static void main(String[] argv) {
        Connection connection = null;
        Channel channel = null;
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(IP);
            factory.setUsername(USERNAME);
            factory.setPassword(PASSWORD);
            factory.setPort(QUEUE_PORT);
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(RPC_REQUEST_QUEUE_NAME, false, false, false, null);

            // 设置每个消费这最多处理一个任务
            channel.basicQos(1);

            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(RPC_REQUEST_QUEUE_NAME, false, consumer);
            System.out.println(" [x] Awaiting RPC requests");

            while (true) {
                String response = null;
                // Main application-side API: wait for the next message delivery and return it.
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();

                BasicProperties requestProps = delivery.getProperties();
                // 相应只要指定请求ID即可
                BasicProperties replyProps = new BasicProperties
                        .Builder()
                        .correlationId(requestProps.getCorrelationId())
                        .build();
                try {
                    String message = new String(delivery.getBody(),"UTF-8");
                    int n = Integer.parseInt(message);

                    System.out.println(" [.] fib(" + message + ")");
                    response = "" + fib(n);
                }
                catch (Exception e){
                    System.out.println(" [.] " + e.toString());
                    response = "";
                }
                finally {
                    // 写入response队列
                    channel.basicPublish( "", requestProps.getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            }
        }
        catch  (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (Exception ignore) {}
            }
        }
    }
}