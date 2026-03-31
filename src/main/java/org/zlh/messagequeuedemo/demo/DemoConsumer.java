package org.zlh.messagequeuedemo.demo;

import lombok.extern.slf4j.Slf4j;
import org.zlh.messagequeuedemo.common.constant.ConstantForMQClientTest;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.mqclient.client.Channel;
import org.zlh.messagequeuedemo.mqclient.client.Connect;
import org.zlh.messagequeuedemo.mqclient.client.ConnectionFactory;
import org.zlh.messagequeuedemo.mqserver.core.ExchangeTtype;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author pluchon
 * @create 2026-03-31-17:52
 *         作者代码水平一般，难免难看，请见谅
 */
// 消费者演示程序
// 使用方式：先启动 MessageQueueDemoApplication，再启动 DemoConsumer，最后启动 DemoProducer
@Slf4j
public class DemoConsumer {
    // 统计已消费的消息总数
    private static final AtomicInteger consumeCount = new AtomicInteger(0);

    public static void main(String[] args) throws IOException, MQException, InterruptedException {
        log.info("========================================");
        log.info("       MQ 消费者 Demo 启动中...");
        log.info("========================================");

        // 1. 建立连接
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        log.info("[消费者] 连接建立成功！channelId={}", channel.getChannelId());

        // 2. 声明交换机和队列（幂等操作，保证消费者先于生产者启动也不出错）
        channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1, ExchangeTtype.DIRECT,
                true, false, null);
        channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1, true, false, false, null);
        log.info("[消费者] 交换机和队列声明完成！");

        // 3. 订阅队列，设置回调函数
        channel.basicConsume(ConstantForMQClientTest.QUEUE_TEST_NAME_1, true, (consumerType, basicProperties, body) -> {
            int count = consumeCount.incrementAndGet();
            String bodyString = new String(body);
            log.info("┌──────────────────────────────────────");
            log.info("│ [消费者] 收到第 {} 条消息", count);
            log.info("│  consumerTag : {}", consumerType);
            log.info("│  messageId   : {}", basicProperties.getMessageID());
            log.info("│  routingKey  : {}", basicProperties.getRoutingKey());
            log.info("│  deliverMode : {}", basicProperties.getDeliverMode());
            log.info("│  消息内容     : {}", bodyString);
            log.info("│  消息大小     : {} 字节", body.length);
            log.info("└──────────────────────────────────────");
        });

        log.info("[消费者] 订阅成功！等待生产者发送消息...");
        log.info("[消费者] 提示：请启动 DemoProducer 发送消息");
        log.info("========================================");

        // 4. 持续等待消费，模拟长时间运行的消费者服务
        // 每隔5秒打印一次心跳日志，表示消费者仍在运行
        while (true) {
            Thread.sleep(5000);
            log.info("[消费者] 心跳 | 已累计消费 {} 条消息，持续监听中...", consumeCount.get());
        }
    }
}
