package org.zlh.messagequeuedemo.demo;

import lombok.extern.slf4j.Slf4j;
import org.zlh.messagequeuedemo.common.constant.ConstantForMQClientTest;
import org.zlh.messagequeuedemo.mqclient.client.Channel;
import org.zlh.messagequeuedemo.mqclient.client.Connect;
import org.zlh.messagequeuedemo.mqclient.client.ConnectionFactory;
import org.zlh.messagequeuedemo.mqserver.core.ExchangeTtype;

import java.io.IOException;

/**
 * @author pluchon
 * @create 2026-03-31-17:52
 *         作者代码水平一般，难免难看，请见谅
 */
// 生产者演示程序
// 使用方式：先启动 MessageQueueDemoApplication，再启动 DemoConsumer，最后启动 DemoProducer
@Slf4j
public class DemoProducer {
    // 模拟发送的消息总数
    private static final int MESSAGE_COUNT = 10;

    public static void main(String[] args) throws IOException, InterruptedException {
        log.info("========================================");
        log.info("       MQ 生产者 Demo 启动中...");
        log.info("========================================");

        // 1. 建立连接
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);
        Connect connect = connectionFactory.newConnection();
        Channel channel = connect.createChannel();
        log.info("[生产者] 连接建立成功！channelId={}", channel.getChannelId());

        // 2. 声明交换机和队列（幂等操作，消费者也会声明，保证谁先启动都不出错）
        channel.exchangeDeclare(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1, ExchangeTtype.DIRECT,
                true, false, null);
        channel.queueDeclare(ConstantForMQClientTest.QUEUE_TEST_NAME_1, true, false, false, null);
        log.info("[生产者] 交换机和队列声明完成！");

        // 3. 循环发送多条消息，模拟真实业务场景
        log.info("[生产者] 开始发送 {} 条消息...", MESSAGE_COUNT);
        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            String messageBody = "消息编号_" + i + "_" + System.currentTimeMillis();
            byte[] content = messageBody.getBytes();
            boolean isOk = channel.basicPublish(ConstantForMQClientTest.EXCHANGE_TEST_NAME_1,
                    ConstantForMQClientTest.QUEUE_TEST_NAME_1, null, content);
            log.info("[生产者] 第 {}/{} 条消息投递{}：{}", i, MESSAGE_COUNT, isOk ? "成功 ✓" : "失败 ✗", messageBody);
            // 模拟业务间隔，方便观察消费者逐条消费
            Thread.sleep(300);
        }

        log.info("========================================");
        log.info("  [生产者] 全部 {} 条消息投递完成！", MESSAGE_COUNT);
        log.info("========================================");

        // 4. 清理资源
        Thread.sleep(500);
        channel.deleteChannel();
        connect.close();
        log.info("[生产者] 连接已关闭，程序退出。");
    }
}
