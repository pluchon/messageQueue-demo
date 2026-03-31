package org.zlh.messagequeuedemo.mqclient.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.zlh.messagequeuedemo.MessageQueueDemoApplication;
import org.zlh.messagequeuedemo.common.constant.ConstantForMQClientTest;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.mqserver.BrokerServer;
import org.zlh.messagequeuedemo.mqserver.core.ExchangeTtype;

import java.io.File;
import java.io.IOException;

/**
 * @author pluchon
 * @create 2026-03-31-17:04
 * 作者代码水平一般，难免难看，请见谅
 */
//统一测试mqclient
@SpringBootTest
@Slf4j
class MQClientTest {
    private BrokerServer brokerServer = null;
    private ConnectionFactory connectionFactory = null;
    private Thread t = null;

    @BeforeEach
    public void setUp() throws IOException {
        //启动服务器
        MessageQueueDemoApplication.context = SpringApplication.run(MessageQueueDemoApplication.class);
        brokerServer = new BrokerServer(ConstantForMQClientTest.port);
        t = new Thread(()->{
            try {
                brokerServer.start();
            } catch (IOException e) {
                log.error("[MQClientTest] 线程启动失败！");
            }
            //start死循环会变得下面代码无法执行！因此我们可创建新的线程执行start就好了
        });
        t.start();

        //配置connectFactory
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(ConstantForMQClientTest.port);
    }

    @AfterEach
    public void tearDown(){
        //停止服务器
        try {
            brokerServer.stop();
            t.join();
            MessageQueueDemoApplication.context.close();
            //删除必要的文件
            File file = new File("./data");
            FileUtils.deleteDirectory(file);
            connectionFactory = null;
        } catch (IOException | InterruptedException e) {
            log.error("[MQClientTest] 资源清理失败！");
        }
    }

    @Test
    public void testConncct() throws IOException {
        Connect connect = connectionFactory.newConnection();
        Assertions.assertNotNull(connect);
    }

    @Test
    public void testChannel() throws IOException {
        Connect connect = connectionFactory.newConnection();
        Assertions.assertNotNull(connect);
        Channel channel = connect.createChannel();
        Assertions.assertNotNull(channel);
    }

    @Test
    public void testExchange() throws IOException {
        Connect connect = connectionFactory.newConnection();
        Assertions.assertNotNull(connect);
        Channel channel = connect.createChannel();
        Assertions.assertNotNull(channel);
        boolean isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1, ExchangeTtype.DIRECT
                , true, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel.exchangeDelete(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
        //加上关闭，稳妥起见
        channel.deleteChannel();
        connect.close();
    }

    @Test
    public void testQueue() throws IOException {
        Connect connect = connectionFactory.newConnection();
        Assertions.assertNotNull(connect);
        Channel channel = connect.createChannel();
        Assertions.assertNotNull(channel);
        boolean isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1
                ,true,false,false,null);
        Assertions.assertTrue(isOk);
        isOk = channel.queueDelete(ConstantForMQClientTest.QUEUE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
        //加上关闭，稳妥起见
        channel.deleteChannel();
        connect.close();
    }

    @Test
    public void testBingding() throws IOException {
        Connect connect = connectionFactory.newConnection();
        Assertions.assertNotNull(connect);
        Channel channel = connect.createChannel();
        Assertions.assertNotNull(channel);
        boolean isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1
                ,true,false,false,null);
        Assertions.assertTrue(isOk);
        isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1, ExchangeTtype.DIRECT
                , true, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel.bingdingDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1
                ,ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,ConstantForMQClientTest.BINGDING_KEY_TEST_1);
        Assertions.assertTrue(isOk);
        isOk = channel.bingdingDelete(ConstantForMQClientTest.QUEUE_TEST_NAME_1,ConstantForMQClientTest.EXCHANGE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
        //加上关闭，稳妥起见
        channel.deleteChannel();
        connect.close();
    }

    @Test
    public void testMessage() throws IOException, MQException, InterruptedException {
        Connect connect = connectionFactory.newConnection();
        Assertions.assertNotNull(connect);
        Channel channel = connect.createChannel();
        Assertions.assertNotNull(channel);
        boolean isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1
                ,true,false,false,null);
        Assertions.assertTrue(isOk);
        isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1, ExchangeTtype.DIRECT
                , true, false, null);
        Assertions.assertTrue(isOk);
        byte[] content = ConstantForMQClientTest.MESSAGE_CONTENT_TEST_1.getBytes();
        isOk = channel.basicPublish(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1
                ,ConstantForMQClientTest.QUEUE_TEST_NAME_1,null,content);
        Assertions.assertTrue(isOk);
        isOk = channel.basicConsume(ConstantForMQClientTest.QUEUE_TEST_NAME_1, true, (consumerType, basicProperties, body) -> {
            log.info("[消费数据] 开始！");
            log.info("[consumerTag] = {},basicProperties={}",consumerType,basicProperties);
            Assertions.assertArrayEquals(content,body);
            log.info("[消费数据] 结束！");
        });
        Assertions.assertTrue(isOk);
        //给时间消费数据
        Thread.sleep(500);
        //加上关闭，稳妥起见
        channel.deleteChannel();
        connect.close();
    }

    // -------- 1. 边界条件：同一个连接创建多个Channel --------
    @Test
    public void testMultipleChannels() throws IOException {
        Connect connect = connectionFactory.newConnection();
        Assertions.assertNotNull(connect);
        Channel channel1 = connect.createChannel();
        Assertions.assertNotNull(channel1);
        Channel channel2 = connect.createChannel();
        Assertions.assertNotNull(channel2);
        //两个channel应该有不同的ID
        Assertions.assertNotEquals(channel1.getChannelId(), channel2.getChannelId());
        //分别在两个channel上操作
        boolean isOk = channel1.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel2.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        //清理
        channel1.deleteChannel();
        channel2.deleteChannel();
        connect.close();
    }

    // -------- 2. 边界条件：Fanout交换机广播消费 --------
    @Test
    public void testFanoutExchange() throws IOException, MQException, InterruptedException {
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        Assertions.assertNotNull(channel);
        //创建fanout交换机
        boolean isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.FINOUT, true, false, null);
        Assertions.assertTrue(isOk);
        //创建队列并绑定
        isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel.bingdingDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                ConstantForMQClientTest.EXCHANGE_TEST_NAME_1, "");
        Assertions.assertTrue(isOk);
        //发送消息（fanout不需要routingKey）
        byte[] content = ConstantForMQClientTest.MESSAGE_CONTENT_TEST_1.getBytes();
        isOk = channel.basicPublish(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                "", null, content);
        Assertions.assertTrue(isOk);
        //订阅消费
        isOk = channel.basicConsume(ConstantForMQClientTest.QUEUE_TEST_NAME_1, true,
                (consumerType, basicProperties, body) -> {
                    log.info("[Fanout消费] consumerTag={}", consumerType);
                    Assertions.assertArrayEquals(content, body);
                });
        Assertions.assertTrue(isOk);
        Thread.sleep(500);
        channel.deleteChannel();
        connect.close();
    }

    // -------- 3. 边界条件：Topic交换机路由消费 --------
    @Test
    public void testTopicExchange() throws IOException, MQException, InterruptedException {
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        Assertions.assertNotNull(channel);
        //创建topic交换机
        boolean isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.TYPOIC, true, false, null);
        Assertions.assertTrue(isOk);
        //创建队列并绑定（使用通配符bindingKey）
        isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel.bingdingDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ConstantForMQClientTest.BINGDING_KEY_FOR_TOPIC_TEST_1);
        Assertions.assertTrue(isOk);
        //发送消息（routingKey应匹配bindingKey aaa.*.bbb）
        byte[] content = ConstantForMQClientTest.MESSAGE_CONTENT_TEST_1.getBytes();
        isOk = channel.basicPublish(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ConstantForMQClientTest.ROUTING_KEY_FOR_TOPIC_TEST_1, null, content);
        Assertions.assertTrue(isOk);
        //订阅消费
        isOk = channel.basicConsume(ConstantForMQClientTest.QUEUE_TEST_NAME_1, true,
                (consumerType, basicProperties, body) -> {
                    log.info("[Topic消费] consumerTag={}", consumerType);
                    Assertions.assertArrayEquals(content, body);
                });
        Assertions.assertTrue(isOk);
        Thread.sleep(500);
        channel.deleteChannel();
        connect.close();
    }

    // -------- 4. 边界条件：手动ACK流程 --------
    @Test
    public void testManualAck() throws IOException, MQException, InterruptedException {
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        Assertions.assertNotNull(channel);
        boolean isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        //发送消息
        byte[] content = ConstantForMQClientTest.MESSAGE_CONTENT_TEST_1.getBytes();
        isOk = channel.basicPublish(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ConstantForMQClientTest.QUEUE_TEST_NAME_1, null, content);
        Assertions.assertTrue(isOk);
        //订阅消费，autoAck=false，手动应答
        isOk = channel.basicConsume(ConstantForMQClientTest.QUEUE_TEST_NAME_1, false,
                (consumerType, basicProperties, body) -> {
                    log.info("[手动ACK消费] consumerTag={}, messageId={}", consumerType, basicProperties.getMessageID());
                    Assertions.assertArrayEquals(content, body);
                    //手动应答
                    try {
                        boolean ackOk = channel.basicAck(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                                basicProperties.getMessageID());
                        Assertions.assertTrue(ackOk);
                        log.info("[手动ACK] 应答成功！");
                    } catch (IOException e) {
                        log.error("[手动ACK] 应答失败！", e);
                    }
                });
        Assertions.assertTrue(isOk);
        Thread.sleep(500);
        channel.deleteChannel();
        connect.close();
    }

    // -------- 5. 边界条件：幂等性-重复创建交换机 --------
    @Test
    public void testExchangeDeclareDuplicate() throws IOException {
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        boolean isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        //再次创建同名交换机
        isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        channel.deleteChannel();
        connect.close();
    }

    // -------- 6. 边界条件：幂等性-重复创建队列 --------
    @Test
    public void testQueueDeclareDuplicate() throws IOException {
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        boolean isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        //再次创建同名队列
        isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        channel.deleteChannel();
        connect.close();
    }

    // -------- 7. 边界条件：完整的生命周期流程（创建-绑定-发布-消费-删除） --------
    @Test
    public void testFullLifecycle() throws IOException, MQException, InterruptedException {
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        //创建交换机和队列
        boolean isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        //建立绑定关系
        isOk = channel.bingdingDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                ConstantForMQClientTest.EXCHANGE_TEST_NAME_1, ConstantForMQClientTest.BINGDING_KEY_TEST_1);
        Assertions.assertTrue(isOk);
        //发送消息
        byte[] content = ConstantForMQClientTest.MESSAGE_CONTENT_TEST_1.getBytes();
        isOk = channel.basicPublish(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ConstantForMQClientTest.QUEUE_TEST_NAME_1, null, content);
        Assertions.assertTrue(isOk);
        //订阅消费
        isOk = channel.basicConsume(ConstantForMQClientTest.QUEUE_TEST_NAME_1, true,
                (consumerType, basicProperties, body) -> {
                    Assertions.assertArrayEquals(content, body);
                    log.info("[完整生命周期] 消费成功！");
                });
        Assertions.assertTrue(isOk);
        Thread.sleep(500);
        //删除绑定关系
        isOk = channel.bingdingDelete(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                ConstantForMQClientTest.EXCHANGE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
        //删除队列
        isOk = channel.queueDelete(ConstantForMQClientTest.QUEUE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
        //删除交换机
        isOk = channel.exchangeDelete(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
        //关闭
        channel.deleteChannel();
        connect.close();
    }

    // -------- 8. 边界条件：连续发送多条消息 --------
    @Test
    public void testPublishMultipleMessages() throws IOException {
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        boolean isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        //发送第一条消息
        isOk = channel.basicPublish(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ConstantForMQClientTest.QUEUE_TEST_NAME_1, null,
                ConstantForMQClientTest.MESSAGE_CONTENT_TEST_1.getBytes());
        Assertions.assertTrue(isOk);
        //发送第二条消息
        isOk = channel.basicPublish(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ConstantForMQClientTest.QUEUE_TEST_NAME_1, null,
                ConstantForMQClientTest.MESSAGE_CONTENT_TEST_2.getBytes());
        Assertions.assertTrue(isOk);
        log.info("[多条消息] 两条消息发送成功！");
        channel.deleteChannel();
        connect.close();
    }

    // -------- 9. 并发条件：多个独立连接同时操作 --------
    @Test
    public void testMultipleConnections() throws IOException {
        Connect connect1 = connectionFactory.newConnection();
        Connect connect2 = connectionFactory.newConnection();
        Assertions.assertNotNull(connect1);
        Assertions.assertNotNull(connect2);
        Channel channel1 = connect1.createChannel();
        Channel channel2 = connect2.createChannel();
        Assertions.assertNotNull(channel1);
        Assertions.assertNotNull(channel2);
        //两个连接各自操作
        boolean isOk1 = channel1.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk1);
        boolean isOk2 = channel2.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk2);
        //清理
        channel1.deleteChannel();
        channel2.deleteChannel();
        connect1.close();
        connect2.close();
    }

    // -------- 10. 边界条件：非持久化资源创建与删除 --------
    @Test
    public void testNonPermanentResources() throws IOException {
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        //创建非持久化交换机
        boolean isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, false, false, null);
        Assertions.assertTrue(isOk);
        //创建非持久化队列
        isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                false, false, false, null);
        Assertions.assertTrue(isOk);
        //删除
        isOk = channel.queueDelete(ConstantForMQClientTest.QUEUE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
        isOk = channel.exchangeDelete(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
        channel.deleteChannel();
        connect.close();
    }

    // -------- 11. 异常情况：Channel重复设置Consumer回调 --------
    @Test
    public void testDuplicateConsumerOnChannel() throws IOException, MQException, InterruptedException {
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        boolean isOk = channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        //第一次订阅
        isOk = channel.basicConsume(ConstantForMQClientTest.QUEUE_TEST_NAME_1, true,
                (consumerType, basicProperties, body) -> {
                    log.info("[第一个消费者] 收到消息");
                });
        Assertions.assertTrue(isOk);
        //第二次订阅同一个channel应该抛出MQException
        Assertions.assertThrows(MQException.class, () -> {
            channel.basicConsume(ConstantForMQClientTest.QUEUE_TEST_NAME_1, true,
                    (consumerType, basicProperties, body) -> {
                        log.info("[第二个消费者] 不应该到达这里");
                    });
        });
        channel.deleteChannel();
        connect.close();
    }

    // -------- 12. 边界条件：三种交换机类型全覆盖创建与删除 --------
    @Test
    public void testAllExchangeTypes() throws IOException {
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        //DIRECT
        boolean isOk = channel.exchangeDeclare("test_direct_exchange",
                ExchangeTtype.DIRECT, false, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel.exchangeDelete("test_direct_exchange");
        Assertions.assertTrue(isOk);
        //FANOUT
        isOk = channel.exchangeDeclare("test_fanout_exchange",
                ExchangeTtype.FINOUT, false, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel.exchangeDelete("test_fanout_exchange");
        Assertions.assertTrue(isOk);
        //TOPIC
        isOk = channel.exchangeDeclare("test_topic_exchange",
                ExchangeTtype.TYPOIC, false, false, null);
        Assertions.assertTrue(isOk);
        isOk = channel.exchangeDelete("test_topic_exchange");
        Assertions.assertTrue(isOk);
        channel.deleteChannel();
        connect.close();
    }
}