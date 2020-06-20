package com.yr.producer;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
/**
 * 发布订阅实现代码
 * 一定要先订阅然后再发布,才能够接收到消息,和reids的发布订阅一样
 * 查看控制台(注意:一定要先订阅,再发布,这样才能接收到生产者的消息)
 * @Auther: linxinmin
 * @Date: 2020-06-10 16:49
 * @Description:
 * 生产者
 */
public class MQProducer {
    // 连接账号
    private String userName = "admin";
    // 连接密码
    private String password = "123456";
    // 连接地址
    private String brokerURL = "tcp://192.168.1.193:61616";
    // connection的工厂
    private ConnectionFactory factory;
    // 连接对象
    private Connection connection;
    // 一个操作会话
    private Session session;
    // 目的地，其实就是连接到哪个队列，如果是点对点，那么它的实现是Queue，如果是订阅模式，那它的实现是Topic
    private Topic destination;
    // 生产者，就是产生数据的对象
    private MessageProducer producer;

    public static void main(String[] args) {
        MQProducer send = new MQProducer();
        System.out.println(send);
        send.start();
        send.sendTextMessage();

    }

    public void start() {
        try {
            // 根据用户名，密码，url创建一个连接工厂
            factory = new ActiveMQConnectionFactory(userName, password, brokerURL);
            // 从工厂中获取一个连接
            connection = factory.createConnection();
            // 测试过这个步骤不写也是可以的，但是网上的各个文档都写了 connection.start();
            // 创建一个session
            // 第一个参数:是否支持事务，如果为true，则会忽略第二个参数，被jms服务器设置为SESSION_TRANSACTED
            // 第二个参数为false时，paramB的值可为Session.AUTO_ACKNOWLEDGE，Session.CLIENT_ACKNOWLEDGE，DUPS_OK_ACKNOWLEDGE其中一个。
            // Session.AUTO_ACKNOWLEDGE为自动确认，客户端发送和接收消息不需要做额外的工作。哪怕是接收端发生异常，也会被当作正常发送成功。
            // Session.CLIENT_ACKNOWLEDGE为客户端确认。客户端接收到消息后，必须调用javax.jms.Message的acknowledge方法。jms服务器才会当作发送成功，并删除消息。
            // DUPS_OK_ACKNOWLEDGE允许副本的确认模式。一旦接收方应用程序的方法调用从处理消息处返回，会话对象就会确认消息的接收；而且允许重复确认。
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            //点对点与订阅模式唯一不同的地方，就是这一行代码，点对点创建的是Queue，而订阅模式创建的是Topic
            destination = session.createTopic("PubSub");
            // 从session中，获取一个消息生产者
            producer = session.createProducer(destination);
            // 设置生产者的模式，有两种可选
            // DeliveryMode.PERSISTENT 当activemq关闭的时候，队列数据将会被保存
            // DeliveryMode.NON_PERSISTENT 当activemq关闭的时候，队列里面的数据将会被清空
            // producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    /**
     * 发送text消息
     */
    public void sendTextMessage() {
        try {
            // 创建一条消息，当然，消息的类型有很多，如文字，字节，对象等,可以通过session.create..方法来创建出来
            TextMessage textMsg = session.createTextMessage();
            for (int i = 0; i < 1000; i++) {
                textMsg.setText("zhi" + i);
                producer.send(textMsg);// 发送一条消息
            }
            System.out.println("发布消息成功");
            // 即便生产者的对象关闭了，程序还在运行哦 producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }




}
