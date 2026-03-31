package org.zlh.messagequeuedemo.mqclient.client;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.common.utils.serializable.BinaryUtilsForJavaUtils;
import org.zlh.messagequeuedemo.mqclient.consumer.Consumer;
import org.zlh.messagequeuedemo.mqclient.dto.Request;
import org.zlh.messagequeuedemo.mqclient.dto.arguments.BasicArguments;
import org.zlh.messagequeuedemo.mqclient.dto.arguments.virtualhost.*;
import org.zlh.messagequeuedemo.mqclient.vo.BasicReturns;
import org.zlh.messagequeuedemo.mqserver.core.BasicProperties;
import org.zlh.messagequeuedemo.mqserver.core.ExchangeTtype;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author pluchon
 * @create 2026-03-31-14:24
 *         作者代码水平一般，难免难看，请见谅
 */
// 一个逻辑上的连接，TCP上可以管理多个逻辑上的连接
@Getter
@Setter
@Slf4j
public class Channel {
    private String channelId;
    // 当前channel属于哪个连接
    private Connect connect;
    // 用来存储后续客户端收到的服务器的响应
    private ConcurrentHashMap<String, BasicReturns> basicReturnsConcurrentHashMap = new ConcurrentHashMap<>();
    // 客户端的回调方法，也就是当我们队列的消息返回的时候进行回调，规定一个channel只能有一个回调
    private Consumer consumer = null;

    public Channel(String channelId, Connect connect) {
        this.channelId = channelId;
        this.connect = connect;
    }

    // 告知服务器创建了channel，构造type=0x1请求
    public boolean createChannel() throws IOException {
        Request request = new Request();
        request.setType(0x1);
        // 构造一个basicArgument对象
        BasicArguments basicArguments = new BasicArguments();
        basicArguments.setChannelId(channelId);
        basicArguments.setRid(createRID());
        byte[] payload = BinaryUtilsForJavaUtils.toBinary(basicArguments);
        request.setLength(payload.length);
        request.setPayload(payload);
        // 发送
        connect.writeRequest(request);
        // 等待服务器响应
        BasicReturns returns = waiResult(basicArguments.getRid());
        return returns.isOk();
    }

    // 删除channel，通知服务器0x2的请求
    public boolean deleteChannel() throws IOException {
        BasicArguments basicArguments = new BasicArguments();
        basicArguments.setRid(createRID());
        basicArguments.setChannelId(channelId);
        byte[] payload = BinaryUtilsForJavaUtils.toBinary(basicArguments);

        Request request = new Request();
        request.setType(0x2);
        request.setLength(payload.length);
        request.setPayload(payload);

        connect.writeRequest(request);

        BasicReturns basicReturns = waiResult(basicArguments.getRid());
        return basicReturns.isOk();
    }

    // 等待创建channel的响应，且这个响应必须和我们的rid对应上！
    // 反复查询响应的哈希表
    private BasicReturns waiResult(String rid) {
        BasicReturns returns = null;
        while ((returns = basicReturnsConcurrentHashMap.get(rid)) == null) {
            // 说明我们的响应还没回来，阻塞等待.....
            // 等待哪个对象就对哪个对象加锁！
            synchronized (this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    log.error("[Channel] 循环等待异常！{}", rid);
                }
            }
        }
        // 读取成功后删除消息，从表中删除，防止越积越多
        basicReturnsConcurrentHashMap.remove(rid);
        return returns;
    }

    private String createRID() {
        return "R-" + UUID.randomUUID();
    }

    // 创建交换机
    public boolean exchangeDeclare(String exechangeName, ExchangeTtype ttype, boolean isPermanet, boolean autoDelete,
            Map<String, Object> arguments) throws IOException {
        ExchangeDeclareArguments exchangeDeclareArguments = new ExchangeDeclareArguments();
        exchangeDeclareArguments.setRid(createRID());
        exchangeDeclareArguments.setChannelId(channelId);
        exchangeDeclareArguments.setExchangeName(exechangeName);
        exchangeDeclareArguments.setExchangeTtype(ttype);
        exchangeDeclareArguments.setPermanet(isPermanet);
        exchangeDeclareArguments.setDelete(autoDelete);
        exchangeDeclareArguments.setArguments(arguments);
        byte[] payload = BinaryUtilsForJavaUtils.toBinary(exchangeDeclareArguments);
        Request request = new Request();
        request.setType(0x3);
        request.setLength(payload.length);
        request.setPayload(payload);

        connect.writeRequest(request);

        BasicReturns returns = waiResult(exchangeDeclareArguments.getRid());
        return returns.isOk();
    }

    // 删除交换机
    public boolean exchangeDelete(String exchangeName) throws IOException {
        ExchangeDeleteArguments exchangeDeleteArguments = new ExchangeDeleteArguments();
        exchangeDeleteArguments.setRid(createRID());
        exchangeDeleteArguments.setExchangeName(exchangeName);
        exchangeDeleteArguments.setChannelId(channelId);

        byte[] payload = BinaryUtilsForJavaUtils.toBinary(exchangeDeleteArguments);

        Request request = new Request();
        request.setType(0x4);
        request.setLength(payload.length);
        request.setPayload(payload);

        connect.writeRequest(request);
        BasicReturns returns = waiResult(exchangeDeleteArguments.getRid());
        return returns.isOk();
    }

    // 创建队列
    public boolean queueDeclare(String queueName, boolean isPermanet, boolean exclusive, boolean autoDelete,
            Map<String, Object> argument) throws IOException {
        QueueDeclareArguments queueDeclareArguments = new QueueDeclareArguments();
        queueDeclareArguments.setRid(createRID());
        queueDeclareArguments.setChannelId(channelId);
        queueDeclareArguments.setQueueName(queueName);
        queueDeclareArguments.setDelete(autoDelete);
        queueDeclareArguments.setExclusive(exclusive);
        queueDeclareArguments.setPermanet(isPermanet);

        byte[] payload = BinaryUtilsForJavaUtils.toBinary(queueDeclareArguments);

        Request request = new Request();
        request.setType(0x5);
        request.setLength(payload.length);
        request.setPayload(payload);

        connect.writeRequest(request);
        BasicReturns returns = waiResult(queueDeclareArguments.getRid());
        return returns.isOk();
    }

    // 删除队列
    public boolean queueDelete(String queueName) throws IOException {
        QueueDeleteArguments queueDeleteArguments = new QueueDeleteArguments();
        queueDeleteArguments.setQueueName(queueName);
        queueDeleteArguments.setRid(createRID());
        queueDeleteArguments.setChannelId(channelId);

        byte[] payload = BinaryUtilsForJavaUtils.toBinary(queueDeleteArguments);

        Request request = new Request();
        request.setType(0x6);
        request.setLength(payload.length);
        request.setPayload(payload);

        connect.writeRequest(request);

        BasicReturns returns = waiResult(queueDeleteArguments.getRid());
        return returns.isOk();
    }

    // 创建绑定
    public boolean bingdingDeclare(String queueName, String exchange, String bingdingKey) throws IOException {
        QueueBingdingDeclareArguments queueBingdingDeclareArguments = new QueueBingdingDeclareArguments();
        queueBingdingDeclareArguments.setRid(createRID());
        queueBingdingDeclareArguments.setChannelId(channelId);
        queueBingdingDeclareArguments.setQueueName(queueName);
        queueBingdingDeclareArguments.setExchangeName(exchange);
        queueBingdingDeclareArguments.setBingdingKey(bingdingKey);

        byte[] payload = BinaryUtilsForJavaUtils.toBinary(queueBingdingDeclareArguments);

        Request request = new Request();
        request.setType(0x7);
        request.setLength(payload.length);
        request.setPayload(payload);

        connect.writeRequest(request);

        BasicReturns returns = waiResult(queueBingdingDeclareArguments.getRid());
        return returns.isOk();
    }

    // 删除绑定
    public boolean bingdingDelete(String queueName, String exchangeName) throws IOException {
        QueueBingdingDeleteArguments queueBingdingDeleteArguments = new QueueBingdingDeleteArguments();
        queueBingdingDeleteArguments.setRid(createRID());
        queueBingdingDeleteArguments.setChannelId(channelId);
        queueBingdingDeleteArguments.setQueueName(queueName);
        queueBingdingDeleteArguments.setExchangeName(exchangeName);

        byte[] payload = BinaryUtilsForJavaUtils.toBinary(queueBingdingDeleteArguments);

        Request request = new Request();
        request.setType(0x8);
        request.setLength(payload.length);
        request.setPayload(payload);

        connect.writeRequest(request);

        BasicReturns returns = waiResult(queueBingdingDeleteArguments.getRid());
        return returns.isOk();
    }

    // 发送消息
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] content)
            throws IOException {
        BasicPublishArguments basicPublishArguments = new BasicPublishArguments();
        basicPublishArguments.setRid(createRID());
        basicPublishArguments.setChannelId(channelId);
        basicPublishArguments.setExchangeName(exchangeName);
        basicPublishArguments.setRoutingKey(routingKey);
        basicPublishArguments.setBasicProperties(basicProperties);
        basicPublishArguments.setContent(content);

        byte[] payload = BinaryUtilsForJavaUtils.toBinary(basicPublishArguments);

        Request request = new Request();
        request.setType(0x9);
        request.setLength(payload.length);
        request.setPayload(payload);

        connect.writeRequest(request);

        BasicReturns returns = waiResult(basicPublishArguments.getRid());
        return returns.isOk();
    }

    // 订阅消息,consumerTag->channelId
    public boolean basicConsume(String queueName, boolean autoAck, Consumer consumer) throws MQException, IOException {
        // 设置回调，如果不为空说明已经被设置够了
        if (this.consumer != null) {
            throw new MQException("[Channel] 该channel已经设置过了回调！" + channelId);
        }
        this.consumer = consumer;
        BasicConsumeArguments basicConsumeArguments = new BasicConsumeArguments();
        basicConsumeArguments.setRid(createRID());
        basicConsumeArguments.setChannelId(channelId);
        // 此处使用channel表示
        basicConsumeArguments.setConsumerTag(channelId);
        basicConsumeArguments.setAutoAck(autoAck);
        basicConsumeArguments.setQueueName(queueName);

        byte[] payload = BinaryUtilsForJavaUtils.toBinary(basicConsumeArguments);

        Request request = new Request();
        request.setType(0xa);
        request.setLength(payload.length);
        request.setPayload(payload);

        connect.writeRequest(request);

        BasicReturns returns = waiResult(basicConsumeArguments.getRid());
        return returns.isOk();
    }

    // 主动应答，确认消息
    public boolean basicAck(String queueName, String messageId) throws IOException {
        BasicAckArguments basicAckArguments = new BasicAckArguments();
        basicAckArguments.setRid(createRID());
        basicAckArguments.setChannelId(channelId);
        basicAckArguments.setQueueName(queueName);
        basicAckArguments.setMessageId(messageId);

        byte[] payload = BinaryUtilsForJavaUtils.toBinary(basicAckArguments);

        Request request = new Request();
        request.setType(0xb);
        request.setLength(payload.length);
        request.setPayload(payload);

        connect.writeRequest(request);

        BasicReturns returns = waiResult(basicAckArguments.getRid());
        return returns.isOk();
    }

    // 防止服务器的响应结果
    public void putReturns(BasicReturns returns) {
        basicReturnsConcurrentHashMap.put(returns.getRid(), returns);
        // 唤醒等待的线程
        synchronized (this) {
            // 也不知道多少个线程在等待，干脆全唤醒
            notifyAll();
        }
    }
}
