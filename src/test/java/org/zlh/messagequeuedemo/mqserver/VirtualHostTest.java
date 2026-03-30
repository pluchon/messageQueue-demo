package org.zlh.messagequeuedemo.mqserver;

import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.zlh.messagequeuedemo.MessageQueueDemoApplication;
import org.zlh.messagequeuedemo.common.constant.ConstantForVirtualHostTest;
import org.zlh.messagequeuedemo.mqserver.core.ExchangeTtype;

import java.io.File;
import java.io.IOException;

/**
 * @author pluchon
 * @create 2026-03-30-18:32
 * 作者代码水平一般，难免难看，请见谅
 */
//虚拟主机测试类
@SpringBootTest
@Slf4j
class VirtualHostTest {
    private VirtualHost virtualHost = null;

    @BeforeEach
    public void setUp(){
        //测试到了硬盘，需要mybatis，保证初始化完成
        MessageQueueDemoApplication.context = SpringApplication.run(MessageQueueDemoApplication.class);
        virtualHost = new VirtualHost(ConstantForVirtualHostTest.VIRTUAL_HOST_TEST_NAME_1);
    }

    @AfterEach
    public void tearDown() throws IOException {
        //关闭context，也就是关闭服务
        MessageQueueDemoApplication.context.close();
        //删除硬盘目录，不管有没有内容
        File file = new File("./data");
        FileUtils.deleteDirectory(file);
        virtualHost = null;
    }

    //创建交换机
    @Test
    public void testExchangeDeclare(){
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1
                ,ExchangeTtype.DIRECT,true,false,null);
        Assertions.assertTrue(isOk);
    }

    //删除交换机
    @Test
    public void testExchangeDelete(){
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1
                ,ExchangeTtype.DIRECT,true,false,null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.exchangeDelete(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
    }

    //创建队列
    @Test
    public void testQueueDeclare(){
        boolean isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1
                ,true,false,false,null);
        Assertions.assertTrue(isOk);
    }

    //删除队列
    @Test
    public void testQueueDelete(){
        boolean isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1
                ,true,false,false,null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDelete(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
    }

    //创建绑定关系
    @Test
    public void testQueueBingding(){
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1
                ,ExchangeTtype.DIRECT,true,false,null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1
                ,true,false,false,null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,ConstantForVirtualHostTest.BINGDING_KEY_TEST_1);
        Assertions.assertTrue(isOk);
    }

    //删除绑定关系
    @Test
    public void testQueueBingdingDelete(){
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1
                ,ExchangeTtype.DIRECT,true,false,null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1
                ,true,false,false,null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,ConstantForVirtualHostTest.BINGDING_KEY_TEST_1);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.bingdingDelete(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
    }

    //发布消息
    @Test
    public void testBasicPublish(){
        //创建交换机与队列
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1
                ,ExchangeTtype.DIRECT,true,false,null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1
                ,true,false,false,null);
        Assertions.assertTrue(isOk);
        //发布消息，我们测试的是直接交换机，我们都routingKey就是我们要发的队列名字
        isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,ConstantForVirtualHostTest.QUEUE_TEST_NAME_1
                ,null,ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes());
        Assertions.assertTrue(isOk);
    }

    //订阅消息->先发消息后订阅队列，扇出交换机
    @Test
    public void testBasicConsume1() throws InterruptedException {
        //创建交换机与队列
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1
                ,ExchangeTtype.DIRECT,true,false,null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1
                ,true,false,false,null);
        Assertions.assertTrue(isOk);
        //订阅队列
        isOk = virtualHost.basicConsume(ConstantForVirtualHostTest.CONSUMER_TAG_TEST_1, ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                true, (consumerType, basicProperties, body) -> {
                    //消费者自身取设定的回调方法
                    log.info("[VirtualHost] 订阅消息->{}->{}",basicProperties.getMessageID(),new String(body));
                    Assertions.assertEquals(ConstantForVirtualHostTest.BINGDING_KEY_TEST_1,basicProperties.getRoutingKey());
                    //默认是不去持久化的
                    Assertions.assertEquals(1,basicProperties.getDeliverMode());
                    //验证消息内容
                    Assertions.assertArrayEquals(ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes(),body);
                });
        Assertions.assertTrue(isOk);
        //休眠下线程，保证上面订阅执行完成
        Thread.sleep(500);
        //发送消息
        isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,ConstantForVirtualHostTest.QUEUE_TEST_NAME_1
                ,null,ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes());
        Assertions.assertTrue(isOk);
    }

    //订阅消息->先订阅后发消息，扇出交换机
    @Test
    public void testBasicConsumer2() throws InterruptedException {
        //创建交换机与队列，注意是广播交换机，均匀广播消息
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1
                ,ExchangeTtype.FINOUT,true,false,null);
        Assertions.assertTrue(isOk);

        //创建两组队列与绑定关系
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,false,false,false,null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,"");
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_2,false,false,false,null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_2,ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,"");
        Assertions.assertTrue(isOk);

        //在交换机中发布一个消息，但是由于我们是扇出交换机，我们发出了两条ID不同但是内容一样的消息
        isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,""
                ,null,ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes());
        Assertions.assertTrue(isOk);

        //第一个消费者订阅第一个队列
        isOk = virtualHost.basicConsume(ConstantForVirtualHostTest.CONSUMER_TAG_TEST_1, ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                true, (consumerType, basicProperties, body) -> {
                    log.info("[VirtualHost] 订阅队列消息->{}->{}",consumerType,basicProperties.getMessageID());
                    Assertions.assertEquals(ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes(),body);
                });
        Assertions.assertTrue(isOk);

        //确保回调方法执行完毕
        Thread.sleep(500);

        //第二个消费者订阅第二个队列
        isOk = virtualHost.basicConsume(ConstantForVirtualHostTest.CONSUMER_TAG_TEST_2, ConstantForVirtualHostTest.QUEUE_TEST_NAME_2,
                true, (consumerType, basicProperties, body) -> {
                    log.info("[VirtualHost] 订阅队列消息->{}->{}",consumerType,basicProperties.getMessageID());
                    Assertions.assertEquals(ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes(),body);
                });
        Assertions.assertTrue(isOk);

        //确保回调方法执行完毕
        Thread.sleep(500);
    }

    //测试主题交换机
    @Test
    public void testBasicConsumeTopic() throws InterruptedException {
        //创建主题交换机
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1
                ,ExchangeTtype.TYPOIC,true,false,null);
        Assertions.assertTrue(isOk);
        //创建队列
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,false,false,false,null);
        Assertions.assertTrue(isOk);
        //建立绑定关系，制定好绑定关系
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1
                ,ConstantForVirtualHostTest.BINGDING_KEY_FOR_TOPIC_TEST_1);
        Assertions.assertTrue(isOk);
        //发送消息
        isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,ConstantForVirtualHostTest.ROUTING_KEY_FOR_TOPIC_TEST_1
                ,null,ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes());
        Assertions.assertTrue(isOk);
        //订阅消息
        isOk = virtualHost.basicConsume(ConstantForVirtualHostTest.CONSUMER_TAG_TEST_1, ConstantForVirtualHostTest.QUEUE_TEST_NAME_1
                , true, (consumerType, basicProperties, body) -> {
                    log.info("[VirtualHost] 订阅队列消息->{}->{}",consumerType,basicProperties.getMessageID());
                    Assertions.assertEquals(ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes(),body);
                });
        Assertions.assertTrue(isOk);

        //确保回调方法执行完毕
        Thread.sleep(500);
    }

    //测试手动调用basicAck
    @Test
    public void testBasciAck() throws InterruptedException {
        //创建交换机与队列
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1
                ,ExchangeTtype.DIRECT,true,false,null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1
                ,true,false,false,null);
        Assertions.assertTrue(isOk);

        //休眠下线程，保证上面代码执行完成
        Thread.sleep(500);

        //订阅队列
        isOk = virtualHost.basicConsume(ConstantForVirtualHostTest.CONSUMER_TAG_TEST_1, ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                false, (consumerType, basicProperties, body) -> {
                    //消费者自身取设定的回调方法
                    log.info("[VirtualHost] 订阅消息->{}->{}", basicProperties.getMessageID(), new String(body));
                    Assertions.assertEquals(ConstantForVirtualHostTest.BINGDING_KEY_TEST_1, basicProperties.getRoutingKey());
                    //默认是不去持久化的
                    Assertions.assertEquals(1, basicProperties.getDeliverMode());
                    //验证消息内容
                    Assertions.assertArrayEquals(ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes(), body);

                    //针对autoAck调用
                    boolean ok = virtualHost.basicAck(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1, basicProperties.getMessageID());
                    Assertions.assertTrue(ok);
                });
        Assertions.assertTrue(isOk);

        //发送消息
        isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1, ConstantForVirtualHostTest.QUEUE_TEST_NAME_1
                ,null, ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes());
        Assertions.assertTrue(isOk);

        //休眠下线程，保证上面代码执行完成
        Thread.sleep(500);
    }

    // -------- 1. 边界条件：幂等性测试 --------
    //重复创建同名交换机（应返回true，幂等）
    @Test
    public void testExchangeDeclareDuplicate() {
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        //再次创建相同名字的交换机
        isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
    }

    //重复创建同名队列（应返回true，幂等）
    @Test
    public void testQueueDeclareDuplicate() {
        boolean isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        //再次创建相同名字的队列
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
    }

    //重复创建同一绑定关系（应返回true，幂等）
    @Test
    public void testBingdingDeclareDuplicate() {
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1, ConstantForVirtualHostTest.BINGDING_KEY_TEST_1);
        Assertions.assertTrue(isOk);
        //再次创建相同的绑定关系
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1, ConstantForVirtualHostTest.BINGDING_KEY_TEST_1);
        Assertions.assertTrue(isOk);
    }

    // -------- 2. 异常情况：删除不存在的资源 --------
    //删除不存在的交换机（应返回false）
    @Test
    public void testDeleteNonExistentExchange() {
        boolean isOk = virtualHost.exchangeDelete(ConstantForVirtualHostTest.EXCHANGE_NOT_EXIST);
        Assertions.assertFalse(isOk);
    }

    //删除不存在的队列（应返回false）
    @Test
    public void testDeleteNonExistentQueue() {
        boolean isOk = virtualHost.queueDelete(ConstantForVirtualHostTest.QUEUE_NOT_EXIST);
        Assertions.assertFalse(isOk);
    }

    //删除不存在的绑定关系（应返回false）
    @Test
    public void testDeleteNonExistentBingding() {
        boolean isOk = virtualHost.bingdingDelete(ConstantForVirtualHostTest.QUEUE_NOT_EXIST,
                ConstantForVirtualHostTest.EXCHANGE_NOT_EXIST);
        Assertions.assertFalse(isOk);
    }

    // -------- 3. 异常情况：绑定不存在的交换机或队列 --------
    //绑定时交换机不存在（应返回false）
    @Test
    public void testBingdingWithNonExistentExchange() {
        boolean isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                ConstantForVirtualHostTest.EXCHANGE_NOT_EXIST, ConstantForVirtualHostTest.BINGDING_KEY_TEST_1);
        Assertions.assertFalse(isOk);
    }

    //绑定时队列不存在（应返回false）
    @Test
    public void testBingdingWithNonExistentQueue() {
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_NOT_EXIST,
                ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1, ConstantForVirtualHostTest.BINGDING_KEY_TEST_1);
        Assertions.assertFalse(isOk);
    }

    // -------- 4. 异常情况：发布消息到不存在的交换机 --------
    @Test
    public void testPublishToNonExistentExchange() {
        boolean isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_NOT_EXIST,
                ConstantForVirtualHostTest.QUEUE_TEST_NAME_1, null,
                ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes());
        Assertions.assertFalse(isOk);
    }

    // -------- 5. 异常情况：发布消息到不存在的队列（DIRECT交换机） --------
    @Test
    public void testPublishToNonExistentQueue() {
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        //routingKey指向一个不存在的队列
        isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ConstantForVirtualHostTest.QUEUE_NOT_EXIST, null,
                ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes());
        Assertions.assertFalse(isOk);
    }

    // -------- 6. 异常情况：非法的routingKey --------
    @Test
    public void testPublishWithInvalidRoutingKey() {
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        //使用包含特殊字符的routingKey
        isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ConstantForVirtualHostTest.ROUTING_KEY_INVALID, null,
                ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes());
        Assertions.assertFalse(isOk);
    }

    // -------- 7. 边界条件：Topic交换机路由不匹配 --------
    //消息的routingKey与bindingKey不匹配时，消息不应该被投递到队列中
    @Test
    public void testTopicRouteNoMatch() throws InterruptedException {
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.TYPOIC, true, false, null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                false, false, false, null);
        Assertions.assertTrue(isOk);
        //绑定关系使用不匹配的bindingKey
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ConstantForVirtualHostTest.BINGDING_KEY_FOR_TOPIC_NO_MATCH);
        Assertions.assertTrue(isOk);
        //发送消息，routingKey与bindingKey不匹配
        isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ConstantForVirtualHostTest.ROUTING_KEY_FOR_TOPIC_TEST_1, null,
                ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes());
        //消息仍然发送成功（只是没有匹配的队列接收）
        Assertions.assertTrue(isOk);
        //订阅该队列，由于没有匹配的消息投递，回调不应该被触发
        //使用标志位来验证
        final boolean[] callbackInvoked = {false};
        isOk = virtualHost.basicConsume(ConstantForVirtualHostTest.CONSUMER_TAG_TEST_1,
                ConstantForVirtualHostTest.QUEUE_TEST_NAME_1, true,
                (consumerType, basicProperties, body) -> {
                    callbackInvoked[0] = true;
                });
        Assertions.assertTrue(isOk);
        Thread.sleep(500);
        //验证回调没有被触发
        Assertions.assertFalse(callbackInvoked[0]);
    }

    // -------- 8. 异常情况：对不存在的消息进行ACK --------
    @Test
    public void testAckNonExistentMessage() {
        boolean isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        //对不存在的消息ID进行确认
        isOk = virtualHost.basicAck(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                ConstantForVirtualHostTest.MESSAGE_ID_NOT_EXIST);
        Assertions.assertFalse(isOk);
    }

    // -------- 9. 异常情况：对不存在的队列进行ACK --------
    @Test
    public void testAckNonExistentQueue() {
        boolean isOk = virtualHost.basicAck(ConstantForVirtualHostTest.QUEUE_NOT_EXIST,
                ConstantForVirtualHostTest.MESSAGE_ID_NOT_EXIST);
        Assertions.assertFalse(isOk);
    }

    // -------- 10. 边界条件：非持久化交换机和队列的创建与删除 --------
    @Test
    public void testNonPermanentExchangeAndQueue() {
        //创建非持久化交换机（不写入硬盘）
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, false, false, null);
        Assertions.assertTrue(isOk);
        //创建非持久化队列（不写入硬盘）
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                false, false, false, null);
        Assertions.assertTrue(isOk);
        //删除非持久化交换机
        isOk = virtualHost.exchangeDelete(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
        //删除非持久化队列
        isOk = virtualHost.queueDelete(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
    }

    // -------- 11. 边界条件：连续发布多条消息到同一队列 --------
    @Test
    public void testPublishMultipleMessages() {
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, true, false, null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                true, false, false, null);
        Assertions.assertTrue(isOk);
        //发送第一条消息
        isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ConstantForVirtualHostTest.QUEUE_TEST_NAME_1, null,
                ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes());
        Assertions.assertTrue(isOk);
        //发送第二条消息
        isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ConstantForVirtualHostTest.QUEUE_TEST_NAME_1, null,
                ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_2.getBytes());
        Assertions.assertTrue(isOk);
    }

    // -------- 12. 并发条件：多线程同时创建交换机 --------
    @Test
    public void testConcurrentExchangeDeclare() throws InterruptedException {
        final int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        boolean[] results = new boolean[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int idx = i;
            threads[i] = new Thread(() -> {
                results[idx] = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                        ExchangeTtype.DIRECT, true, false, null);
            });
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        //所有线程都应该返回true（幂等创建）
        for (boolean result : results) {
            Assertions.assertTrue(result);
        }
    }

    // -------- 13. 并发条件：多线程同时创建队列 --------
    @Test
    public void testConcurrentQueueDeclare() throws InterruptedException {
        final int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        boolean[] results = new boolean[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int idx = i;
            threads[i] = new Thread(() -> {
                results[idx] = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                        true, false, false, null);
            });
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        //所有线程都应该返回true（幂等创建）
        for (boolean result : results) {
            Assertions.assertTrue(result);
        }
    }

    // -------- 14. 边界条件：先删除交换机再删除绑定关系（方案二验证） --------
    @Test
    public void testDeleteExchangeBeforeBingding() {
        //创建全套
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, false, false, null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                false, false, false, null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1, ConstantForVirtualHostTest.BINGDING_KEY_TEST_1);
        Assertions.assertTrue(isOk);
        //先删交换机
        isOk = virtualHost.exchangeDelete(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
        //再删绑定关系（交换机已不存在，方案二下应该仍然能删）
        isOk = virtualHost.bingdingDelete(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1);
        //根据当前实现，先查绑定关系是否在内存中存在，交换机不在了但绑定仍在内存中
        //具体结果取决于实现，此处验证不会抛异常崩溃即可
        log.info("[VirtualHostTest] 先删交换机再删绑定关系结果: {}", isOk);
    }

    // -------- 15. 边界条件：先删除队列再删除绑定关系（方案二验证） --------
    @Test
    public void testDeleteQueueBeforeBingding() {
        //创建全套
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.DIRECT, false, false, null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.queueDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                false, false, false, null);
        Assertions.assertTrue(isOk);
        isOk = virtualHost.bingdingDeclare(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1, ConstantForVirtualHostTest.BINGDING_KEY_TEST_1);
        Assertions.assertTrue(isOk);
        //先删队列
        isOk = virtualHost.queueDelete(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1);
        Assertions.assertTrue(isOk);
        //再删绑定关系（队列已不存在）
        isOk = virtualHost.bingdingDelete(ConstantForVirtualHostTest.QUEUE_TEST_NAME_1,
                ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1);
        log.info("[VirtualHostTest] 先删队列再删绑定关系结果: {}", isOk);
    }

    // -------- 16. 边界条件：发布消息到没有任何绑定的fanout交换机 --------
    @Test
    public void testPublishToFanoutWithNoBindings() {
        boolean isOk = virtualHost.exchangeDeclare(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                ExchangeTtype.FINOUT, true, false, null);
        Assertions.assertTrue(isOk);
        //不创建任何队列和绑定关系，直接发送消息
        //消息应该发送成功，但不会被投递到任何队列
        isOk = virtualHost.basicPublish(ConstantForVirtualHostTest.EXCHANGE_TEST_NAME_1,
                "", null, ConstantForVirtualHostTest.MESSAGE_CONTENT_TEST_1.getBytes());
        //注意：如果 queryAllBingding 返回 null，此处可能会失败，这也是一个需要验证的边界条件
        log.info("[VirtualHostTest] 无绑定的fanout交换机发布消息结果: {}", isOk);
    }
}