package org.zlh.messagequeuedemo.mqserver.datacenter;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.zlh.messagequeuedemo.MessageQueueDemoApplication;
import org.zlh.messagequeuedemo.common.constant.ConstantForDiskDataCenterTest;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.mqserver.core.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author pluchon
 * @create 2026-03-28-10:47
 *         作者代码水平一般，难免难看，请见谅
 */
@Slf4j
@SpringBootTest
public class DiskDataCenterTest {
    private final DiskDataCenter diskDataCenter = new DiskDataCenter();

    @BeforeEach
    public void setUp() {
        //启动 Spring 上下文，初始化数据库和文件目录
        MessageQueueDemoApplication.context = SpringApplication.run(MessageQueueDemoApplication.class);
        diskDataCenter.init();
        log.info("[setUp] DiskDataCenter 初始化完成");
    }

    @AfterEach
    public void tearDown() throws IOException {
        //关闭 Spring 上下文，释放数据库文件锁，再清理磁盘数据
        MessageQueueDemoApplication.context.close();
        //清理数据库文件
        DataBaseManager dataBaseManager = new DataBaseManager();
        dataBaseManager.deleteDB();
        //清理消息文件目录：把测试中创建的两个队列目录删掉
        MessageFileManager messageFileManager = new MessageFileManager();
        //deleteQueue 要求文件必须存在才能删，我们用 try-catch 兜底，避免因为测试失败导致目录不存在而二次报错
        try {
            messageFileManager.deleteQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_1);
        } catch (IOException ignored) {
            //.....
        }
        try {
            messageFileManager.deleteQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_2);
        } catch (IOException ignored) {
            //.....
        }
        log.info("[tearDown] 磁盘数据清理完成");
    }

    // ======================== 构建辅助工具 ========================

    //构建交换机
    private Exchange createExchange(String exchangeName) {
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setExchangeType(ExchangeTtype.DIRECT);
        exchange.setIsPermanent(true);
        exchange.setIsDelete(false);
        return exchange;
    }

    //构建队列
    private MSGQueue createQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setIsPermanent(true);
        queue.setIsDelete(false);
        queue.setExclusivel(false);
        return queue;
    }

    //构建绑定关系
    private Bingding createBingding(String exchangeName, String queueName, String bindingKey) {
        Bingding bingding = new Bingding();
        bingding.setExchangeName(exchangeName);
        bingding.setQueueName(queueName);
        bingding.setBindingKey(bindingKey);
        return bingding;
    }

    //构建消息
    private Message createMessage(String content) {
        return Message.messageCreateWithIDFactory(ConstantForDiskDataCenterTest.ROUTING_KEY_1, null, content.getBytes());
    }

    // ======================== 交换机测试 ========================

    @Test
    public void testInsertAndQueryExchange() {
        Exchange exchange = createExchange(ConstantForDiskDataCenterTest.EXCHANGE_NAME_1);
        diskDataCenter.insertExchange(exchange);
        log.info("[testInsertAndQueryExchange] 插入交换机：{}", exchange.getName());
        List<Exchange> exchangeList = diskDataCenter.queryAllExchange();
        //初始有一个默认匿名交换机，插入后应有 2 条
        Assertions.assertEquals(2, exchangeList.size());
        Exchange result = exchangeList.get(1);
        Assertions.assertEquals(ConstantForDiskDataCenterTest.EXCHANGE_NAME_1, result.getName());
        Assertions.assertEquals(ExchangeTtype.DIRECT, result.getExchangeType());
        Assertions.assertTrue(result.getIsPermanent());
        Assertions.assertFalse(result.getIsDelete());
        Assertions.assertNotNull(result);
        log.info("[testInsertAndQueryExchange] 交换机校验通过：name={}, type={}", result.getName(), result.getExchangeType());
        log.info("[testInsertAndQueryExchange] 交换机插入与查询校验成功！");
    }

    @Test
    public void testDeleteExchange() {
        Exchange exchange = createExchange(ConstantForDiskDataCenterTest.EXCHANGE_NAME_1);
        diskDataCenter.insertExchange(exchange);
        List<Exchange> beforeDelete = diskDataCenter.queryAllExchange();
        Assertions.assertEquals(2, beforeDelete.size());
        log.info("[testDeleteExchange] 插入后数量校验通过：{} 条", beforeDelete.size());
        diskDataCenter.deleteExchange(ConstantForDiskDataCenterTest.EXCHANGE_NAME_1);
        List<Exchange> afterDelete = diskDataCenter.queryAllExchange();
        //删除后只剩默认匿名交换机
        Assertions.assertEquals(1, afterDelete.size());
        Assertions.assertEquals("", afterDelete.get(0).getName());
        log.info("[testDeleteExchange] 删除后数量校验通过：{} 条", afterDelete.size());
        //删除不存在的交换机，不应该抛异常
        Assertions.assertDoesNotThrow(() -> diskDataCenter.deleteExchange("nonExistExchange"));
        Assertions.assertEquals(1, diskDataCenter.queryAllExchange().size());
        log.info("[testDeleteExchange] 边界校验通过：删除不存在的交换机不抛异常");
        log.info("[testDeleteExchange] 交换机删除校验成功！");
    }

    // ======================== 队列测试 ========================

    @Test
    public void testInsertAndQueryQueue() throws IOException {
        MSGQueue queue = createQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_1);
        diskDataCenter.insertQueue(queue);
        log.info("[testInsertAndQueryQueue] 插入队列：{}", queue.getName());
        List<MSGQueue> queueList = diskDataCenter.queryAllQueue();
        //队列初始为空，插入后应有 1 条
        Assertions.assertEquals(1, queueList.size());
        MSGQueue result = queueList.get(0);
        Assertions.assertEquals(ConstantForDiskDataCenterTest.QUEUE_NAME_1, result.getName());
        Assertions.assertTrue(result.getIsPermanent());
        Assertions.assertFalse(result.getIsDelete());
        Assertions.assertFalse(result.getExclusivel());
        log.info("[testInsertAndQueryQueue] 队列校验通过：name={}", result.getName());
        log.info("[testInsertAndQueryQueue] 队列插入与查询校验成功！");
    }

    @Test
    public void testDeleteQueue() throws IOException {
        MSGQueue queue = createQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_1);
        diskDataCenter.insertQueue(queue);
        Assertions.assertEquals(1, diskDataCenter.queryAllQueue().size());
        log.info("[testDeleteQueue] 插入后数量校验通过：1 条");
        diskDataCenter.deleteQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_1);
        Assertions.assertEquals(0, diskDataCenter.queryAllQueue().size());
        log.info("[testDeleteQueue] 删除后数量校验通过：0 条");
        //删除不存在的队列，deleteQueue 会尝试删文件，文件不存在则抛 IOException，这是明确的错误语义
        Assertions.assertThrows(IOException.class, () -> diskDataCenter.deleteQueue("nonExistQueue"));
        log.info("[testDeleteQueue] 边界校验通过：删除不存在的队列抛出 IOException");
        log.info("[testDeleteQueue] 队列删除校验成功！");
    }

    // ======================== 绑定关系测试 ========================

    @Test
    public void testInsertAndQueryBingding() throws IOException {
        //插入绑定依赖的交换机和队列
        diskDataCenter.insertExchange(createExchange(ConstantForDiskDataCenterTest.EXCHANGE_NAME_1));
        diskDataCenter.insertQueue(createQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_1));
        diskDataCenter.insertQueue(createQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_2));
        Bingding bingding1 = createBingding(ConstantForDiskDataCenterTest.EXCHANGE_NAME_1,
                ConstantForDiskDataCenterTest.QUEUE_NAME_1, ConstantForDiskDataCenterTest.BINDING_KEY_1);
        Bingding bingding2 = createBingding(ConstantForDiskDataCenterTest.EXCHANGE_NAME_1,
                ConstantForDiskDataCenterTest.QUEUE_NAME_2, ConstantForDiskDataCenterTest.BINDING_KEY_1);
        diskDataCenter.insertBingding(bingding1);
        diskDataCenter.insertBingding(bingding2);
        log.info("[testInsertAndQueryBingding] 插入 2 条绑定关系");
        List<Bingding> bingdingList = diskDataCenter.queryAllBingding();
        Assertions.assertEquals(2, bingdingList.size());
        Bingding result = bingdingList.get(0);
        Assertions.assertEquals(ConstantForDiskDataCenterTest.EXCHANGE_NAME_1, result.getExchangeName());
        Assertions.assertEquals(ConstantForDiskDataCenterTest.QUEUE_NAME_1, result.getQueueName());
        Assertions.assertEquals(ConstantForDiskDataCenterTest.BINDING_KEY_1, result.getBindingKey());
        Assertions.assertNotNull(result);
        log.info("[testInsertAndQueryBingding] 绑定关系校验通过：exchange={}, queue={}", result.getExchangeName(), result.getQueueName());
        log.info("[testInsertAndQueryBingding] 绑定关系插入与查询校验成功！");
    }

    @Test
    public void testDeleteBingding() throws IOException {
        diskDataCenter.insertExchange(createExchange(ConstantForDiskDataCenterTest.EXCHANGE_NAME_1));
        diskDataCenter.insertQueue(createQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_1));
        diskDataCenter.insertQueue(createQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_2));
        Bingding bingding1 = createBingding(ConstantForDiskDataCenterTest.EXCHANGE_NAME_1,
                ConstantForDiskDataCenterTest.QUEUE_NAME_1, ConstantForDiskDataCenterTest.BINDING_KEY_1);
        Bingding bingding2 = createBingding(ConstantForDiskDataCenterTest.EXCHANGE_NAME_1,
                ConstantForDiskDataCenterTest.QUEUE_NAME_2, ConstantForDiskDataCenterTest.BINDING_KEY_1);
        diskDataCenter.insertBingding(bingding1);
        diskDataCenter.insertBingding(bingding2);
        Assertions.assertEquals(2, diskDataCenter.queryAllBingding().size());
        //删除第一条，验证只剩一条且是 queue2 的绑定
        diskDataCenter.deleteBingding(bingding1);
        List<Bingding> bingdingList = diskDataCenter.queryAllBingding();
        Assertions.assertEquals(1, bingdingList.size());
        Assertions.assertEquals(ConstantForDiskDataCenterTest.QUEUE_NAME_2, bingdingList.get(0).getQueueName());
        log.info("[testDeleteBingding] 删除第一条后数量校验通过，剩余绑定 queue={}", bingdingList.get(0).getQueueName());
        //删除第二条，验证列表为空
        diskDataCenter.deleteBingding(bingding2);
        Assertions.assertEquals(0, diskDataCenter.queryAllBingding().size());
        //边界：删除不存在的绑定不应该抛异常
        Bingding nonExist = createBingding("nonExist", "nonExist", "nonExist");
        Assertions.assertDoesNotThrow(() -> diskDataCenter.deleteBingding(nonExist));
        Assertions.assertEquals(0, diskDataCenter.queryAllBingding().size());
        log.info("[testDeleteBingding] 边界校验通过：删除不存在的绑定不抛异常");
        log.info("[testDeleteBingding] 绑定关系删除校验成功！");
    }

    // ======================== 消息文件测试 ========================

    @Test
    public void testInsertAndQueryMessage() throws MQException, IOException, ClassNotFoundException {
        //消息文件操作依赖队列目录，需要先插入队列（insertQueue 会触发 createQueue 创建目录）
        MSGQueue queue = createQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_1);
        diskDataCenter.insertQueue(queue);
        Message message = createMessage(ConstantForDiskDataCenterTest.MESSAGE_CONTENT_1 + "0");
        diskDataCenter.insertMessage(queue, message);
        log.info("[testInsertAndQueryMessage] 插入消息 -> messageId={}, body={}",
                message.getMessageId(), new String(message.getBody()));
        LinkedList<Message> messageList = diskDataCenter.queryAllMessage(ConstantForDiskDataCenterTest.QUEUE_NAME_1);
        Assertions.assertEquals(1, messageList.size());
        Message result = messageList.get(0);
        Assertions.assertEquals(message.getMessageId(), result.getMessageId());
        Assertions.assertEquals(message.getRoutingKey(), result.getRoutingKey());
        Assertions.assertArrayEquals(message.getBody(), result.getBody());
        log.info("[testInsertAndQueryMessage] 读回消息校验通过：messageId={}, body={}",
                result.getMessageId(), new String(result.getBody()));
        //读取空队列，结果是空列表，非 null
        MSGQueue queue2 = createQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_2);
        diskDataCenter.insertQueue(queue2);
        LinkedList<Message> emptyList = diskDataCenter.queryAllMessage(ConstantForDiskDataCenterTest.QUEUE_NAME_2);
        Assertions.assertNotNull(emptyList);
        Assertions.assertEquals(0, emptyList.size());
        log.info("[testInsertAndQueryMessage] 边界校验通过：空队列读取返回空列表");
        log.info("[testInsertAndQueryMessage] 消息插入与查询校验成功！");
    }

    @Test
    public void testDeleteMessage() throws MQException, IOException, ClassNotFoundException {
        //先构建好队列和消息
        MSGQueue queue = createQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_1);
        diskDataCenter.insertQueue(queue);
        List<Message> expectedList = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Message msg = createMessage(ConstantForDiskDataCenterTest.MESSAGE_CONTENT_1 + i);
            diskDataCenter.insertMessage(queue, msg);
            expectedList.add(msg);
        }
        log.info("[testDeleteMessage] 已写入 10 条消息");
        //删除最后三条
        diskDataCenter.deleteMessage(queue, expectedList.get(7));
        diskDataCenter.deleteMessage(queue, expectedList.get(8));
        diskDataCenter.deleteMessage(queue, expectedList.get(9));
        log.info("[testDeleteMessage] 已软删除下标 7/8/9 的消息");
        LinkedList<Message> messageList = diskDataCenter.queryAllMessage(ConstantForDiskDataCenterTest.QUEUE_NAME_1);
        Assertions.assertEquals(7, messageList.size());
        //逐条校验剩余的 7 条消息内容正确
        for (int i = 0; i < messageList.size(); i++) {
            Assertions.assertEquals(expectedList.get(i).getMessageId(), messageList.get(i).getMessageId());
            Assertions.assertArrayEquals(expectedList.get(i).getBody(), messageList.get(i).getBody());
        }
        log.info("[testDeleteMessage] 剩余 {} 条消息内容逐条校验通过", messageList.size());
        log.info("[testDeleteMessage] 消息删除校验成功！");
    }

    //测试 deleteMessage 内嵌的自动 GC 逻辑
    //当满足条件（totalCount > 2000 且 validCount/totalCount < 50%）时应自动触发 GC
    //手动触发 GC 需要 totalCount > 2000，我们用 MessageFileManager 直接绕过 DiskDataCenter 写入大量消息
    @Test
    public void testAutoGCTriggeredByDeleteMessage() throws MQException, IOException, ClassNotFoundException {
        //为了快速构造 GC 触发条件，我们直接调用底层 MessageFileManager 写 2001 条消息
        MSGQueue queue = createQueue(ConstantForDiskDataCenterTest.QUEUE_NAME_1);
        diskDataCenter.insertQueue(queue);
        MessageFileManager messageFileManager = new MessageFileManager();
        List<Message> msgList = new LinkedList<>();
        for (int i = 0; i < 2001; i++) {
            Message msg = createMessage(ConstantForDiskDataCenterTest.MESSAGE_CONTENT_1 + i);
            messageFileManager.sendMessage(queue, msg);
            msgList.add(msg);
        }
        log.info("[testAutoGCTriggeredByDeleteMessage] 底层写入 2001 条消息");
        //删除 50% 以上（1001 条），让 checkGC 条件满足
        for (int i = 0; i < 1001; i++) {
            //通过 DiskDataCenter.deleteMessage 触发 checkGC 判断
            diskDataCenter.deleteMessage(queue, msgList.get(i));
        }
        log.info("[testAutoGCTriggeredByDeleteMessage] 已删除 1001 条，GC 应已自动触发");
        //GC 触发后，文件中只剩下有效消息（1000 条），且 totalCount 和 validCount 应该同步为 1000
        LinkedList<Message> messageList = diskDataCenter.queryAllMessage(ConstantForDiskDataCenterTest.QUEUE_NAME_1);
        Assertions.assertEquals(1000, messageList.size());
        log.info("[testAutoGCTriggeredByDeleteMessage] GC 后有效消息数量校验通过：{} 条", messageList.size());
        log.info("[testAutoGCTriggeredByDeleteMessage] 自动 GC 触发校验成功！");
    }
}
