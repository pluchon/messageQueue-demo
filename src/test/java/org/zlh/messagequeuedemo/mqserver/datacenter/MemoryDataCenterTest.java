package org.zlh.messagequeuedemo.mqserver.datacenter;

import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.zlh.messagequeuedemo.MessageQueueDemoApplication;
import org.zlh.messagequeuedemo.common.constant.ConstantForMemoryDataCenterTest;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.mqserver.core.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author pluchon
 * @create 2026-03-28-21:08
 * 作者代码水平一般，难免难看，请见谅
 */
@Slf4j
@SpringBootTest
public class MemoryDataCenterTest {
    private MemoryDataCenter memoryDataCenter = null;

    @BeforeEach
    public void setUp(){
        //为什么要在这里new对象？因为我们这里数据都存储在内存中，可能会有相互冲突！
        memoryDataCenter = new MemoryDataCenter();
    }

    @AfterEach
    public void tearDown(){
        //变成null后直接被垃圾回收掉了
        memoryDataCenter = null;
    }

    //创建测试交换机
    private Exchange createTestExchange(String exchangeName){
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setExchangeType(ExchangeTtype.DIRECT);
        exchange.setIsPermanent(true);
        exchange.setIsDelete(false);
        return exchange;
    }

    //创建测试队列
    private MSGQueue createTestQueue(String queueName){
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setIsPermanent(true);
        queue.setExclusivel(false);
        queue.setIsPermanent(false);
        return queue;
    }

    //创建测试消息
    private Message createMessage(String content){
        return Message.messageCreateWithIDFactory(ConstantForMemoryDataCenterTest.ROUTING_KEY_TEST_NAME_1
                ,null,content.getBytes());
    }

    //=============针对交换机=============
    @Test
    public void testExchange(){
        //创建交换机并插入
        Exchange expectedExchange = createTestExchange(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1);
        memoryDataCenter.insertExchange(expectedExchange);
        log.info("[testExchange] 插入交换机：{}", expectedExchange.getName());
        //查询这个交换机
        Exchange actualExchange = memoryDataCenter.getExchange(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1);
        //对比，无需一个个参数比较，只需比较是否指向同一个对象就好（因为在内存中）
        Assertions.assertEquals(expectedExchange,actualExchange);
        log.info("[testExchange] 查询校验通过，引用相同：{}", actualExchange.getName());
        //删除交换机
        memoryDataCenter.deleteExchange(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1);
        actualExchange = memoryDataCenter.getExchange(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1);
        //对比，应该是空结果
        Assertions.assertNull(actualExchange);
        log.info("[testExchange] 删除后查询为 null，校验通过");
        //查询不存在的交换机，应返回 null 而不是抛异常
        Exchange nonExist = memoryDataCenter.getExchange("nonExistExchange");
        Assertions.assertNull(nonExist);
        log.info("[testExchange] 边界校验通过：查询不存在的交换机返回 null");
        //重复插入同一个对象，后者覆盖前者（引用相同）
        memoryDataCenter.insertExchange(expectedExchange);
        memoryDataCenter.insertExchange(expectedExchange);
        Assertions.assertEquals(expectedExchange, memoryDataCenter.getExchange(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1));
        log.info("[testExchange] 边界校验通过：重复插入不报错，保持最新引用");
        log.info("[testExchange] 交换机测试全部通过！");
    }

    //=====针对队列=======
    @Test
    public void testQueue(){
        MSGQueue expectedQueue = createTestQueue(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        memoryDataCenter.insertQueue(expectedQueue);
        log.info("[testQueue] 插入队列：{}", expectedQueue.getName());
        MSGQueue actualQueue = memoryDataCenter.getQueue(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        Assertions.assertEquals(expectedQueue,actualQueue);
        log.info("[testQueue] 查询校验通过，引用相同：{}", actualQueue.getName());
        //删除队列
        memoryDataCenter.deleteQueue(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        actualQueue = memoryDataCenter.getQueue(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        //对比，应该是空结果
        Assertions.assertNull(actualQueue);
        log.info("[testQueue] 删除后查询为 null，校验通过");
        //查询不存在的队列，应返回 null
        MSGQueue nonExist = memoryDataCenter.getQueue("nonExistQueue");
        Assertions.assertNull(nonExist);
        log.info("[testQueue] 边界校验通过：查询不存在的队列返回 null");
        //删除不存在的队列不应抛异常（内存 ConcurrentHashMap.remove 对不存在的 key 是幂等的）
        Assertions.assertDoesNotThrow(() -> memoryDataCenter.deleteQueue("nonExistQueue"));
        log.info("[testQueue] 边界校验通过：删除不存在的队列不抛异常");
        log.info("[testQueue] 队列测试全部通过！");
    }

    //=====针对绑定关系========
    @Test
    public void testBingding() throws MQException {
        Bingding expectedBingding = new Bingding();
        expectedBingding.setExchangeName(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1);
        expectedBingding.setQueueName(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        expectedBingding.setBindingKey(ConstantForMemoryDataCenterTest.BINGIDNG_KEY_TEST_NAME_1);
        memoryDataCenter.insertBingding(expectedBingding);
        log.info("[testBingding] 插入绑定关系：{} -> {}", expectedBingding.getExchangeName(), expectedBingding.getQueueName());
        //查询
        Bingding actualBingding = memoryDataCenter.getBingdingOnce(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1
                ,ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        //比较
        Assertions.assertEquals(expectedBingding,actualBingding);
        log.info("[testBingding] 单条查询校验通过");
        //测试该交换机的所有绑定关系，此处只有一个关系
        ConcurrentHashMap<String, Bingding> stringBingdingConcurrentHashMap = memoryDataCenter
                .queryAllBingding(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1);
        Assertions.assertEquals(1,stringBingdingConcurrentHashMap.size());
        Assertions.assertEquals(expectedBingding,stringBingdingConcurrentHashMap
                .get(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1));
        log.info("[testBingding] queryAllBingding 数量校验通过：{} 条", stringBingdingConcurrentHashMap.size());
        //删除
        memoryDataCenter.deleteBingding(expectedBingding);
        //比较应该为空
        actualBingding = memoryDataCenter.getBingdingOnce(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1
                ,ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        Assertions.assertNull(actualBingding);
        log.info("[testBingding] 删除后查询为 null，校验通过");
        //重复插入同一个绑定关系，应抛出 MQException（MemoryDataCenter 有重复检测）
        memoryDataCenter.insertBingding(expectedBingding);
        Assertions.assertThrows(MQException.class, () -> memoryDataCenter.insertBingding(expectedBingding));
        log.info("[testBingding] 边界校验通过：重复插入绑定关系抛出 MQException");
        //对一个完全不存在的交换机查询其 queryAllBingding，返回应为 null
        ConcurrentHashMap<String, Bingding> nonExistResult = memoryDataCenter.queryAllBingding("nonExistExchange");
        Assertions.assertNull(nonExistResult);
        log.info("[testBingding] 边界校验通过：查询不存在交换机的绑定关系返回 null");
        log.info("[testBingding] 绑定关系测试全部通过！");
    }

    //针对消息测试
    @Test
    public void testMessage(){
        Message expectedMessage = createMessage(ConstantForMemoryDataCenterTest.MESSAGE_CONTENT_TEST_1);
        memoryDataCenter.insertMessage(expectedMessage);
        log.info("[testMessage] 插入消息，messageId={}", expectedMessage.getMessageId());
        //查询（按 messageId 从总消息表查，应使用 getMessageWithId，而非 getMessage(queueName)）
        Message actualMessage = memoryDataCenter.getMessageWithId(expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage,actualMessage);
        log.info("[testMessage] 查询校验通过，引用相同");
        //删除操作
        memoryDataCenter.deleteMessage(expectedMessage.getMessageId());
        actualMessage = memoryDataCenter.getMessageWithId(expectedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
        log.info("[testMessage] 删除后查询为 null，校验通过");
        //查询不存在的 messageId，返回 null
        Message nonExist = memoryDataCenter.getMessageWithId("nonExistMessageId");
        Assertions.assertNull(nonExist);
        log.info("[testMessage] 边界校验通过：查询不存在的 messageId 返回 null");
        //重复删除同一条消息，不应抛异常（ConcurrentHashMap.remove 是幂等的）
        Assertions.assertDoesNotThrow(() -> memoryDataCenter.deleteMessage(expectedMessage.getMessageId()));
        log.info("[testMessage] 边界校验通过：重复删除消息不抛异常");
        log.info("[testMessage] 消息总表测试全部通过！");
    }

    //测试消息发送
    @Test
    public void testSendMessage(){
        MSGQueue queue = createTestQueue(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        List<Message> expectedMessageList = new ArrayList<>();
        //创建十条消息
        for(int i = 0;i < 10;i++){
            Message messageInfo = createMessage(ConstantForMemoryDataCenterTest.MESSAGE_CONTENT_TEST_1+i);
            memoryDataCenter.sendMessage(queue,messageInfo);
            expectedMessageList.add(messageInfo);
        }
        log.info("[testSendMessage] 已发送 10 条消息到队列：{}", queue.getName());
        //取出消息
        List<Message> actualMessageList = new LinkedList<>();
        while(true){
            Message message = memoryDataCenter.getMessage(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
            //队列获取消息结束
            if(message == null){
                break;
            }
            actualMessageList.add(message);
        }
        log.info("[testSendMessage] 已从队列取出 {} 条消息", actualMessageList.size());
        //逐一比较
        Assertions.assertEquals(expectedMessageList.size(),actualMessageList.size());
        for(int i = 0;i < 10;i++){
            Assertions.assertEquals(expectedMessageList.get(i),actualMessageList.get(i));
        }
        log.info("[testSendMessage] 逐条消息对比通过：FIFO 顺序正确");
        //队列已取空，再调 getMessage 应返回 null 而非抛异常
        Message emptyResult = memoryDataCenter.getMessage(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        Assertions.assertNull(emptyResult);
        log.info("[testSendMessage] 边界校验通过：队列取空后继续取返回 null");
        //对从未 sendMessage 的队列直接取消息，应返回 null 而非 NPE
        Message neverSentResult = memoryDataCenter.getMessage("neverUsedQueue");
        Assertions.assertNull(neverSentResult);
        log.info("[testSendMessage] 边界校验通过：从未写入的队列取消息返回 null");
        //发送消息后总消息表里也应该有记录（sendMessage 内部会调用 insertMessage）
        Message msgInGlobalTable = memoryDataCenter.getMessageWithId(expectedMessageList.get(0).getMessageId());
        //消息已经通过 getMessage 取出后从队列链表移走了，但总消息表里仍存在
        Assertions.assertNotNull(msgInGlobalTable);
        log.info("[testSendMessage] 总消息表校验通过：sendMessage 同步写入了总消息表");
        log.info("[testSendMessage] 消息发送测试全部通过！");
    }

    //测试未被确认的消息
    @Test
    public void testAckWithMessage(){
        Message expectedMessage = createMessage(ConstantForMemoryDataCenterTest.MESSAGE_CONTENT_TEST_1);
        memoryDataCenter.insertWithAckMessage(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1,expectedMessage);
        log.info("[testAckWithMessage] 插入未确认消息，queueName={}, messageId={}",
                ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1, expectedMessage.getMessageId());
        //获取
        Message actualMessage = memoryDataCenter
                .getWithAckMessage(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1, expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage,actualMessage);
        log.info("[testAckWithMessage] 查询校验通过，引用相同");
        //删除
        memoryDataCenter.deleteWithAckMessage(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1,expectedMessage);
        actualMessage = memoryDataCenter.getWithAckMessage(ConstantForMemoryDataCenterTest
                .QUEUE_TEST_NAME_1,expectedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
        log.info("[testAckWithMessage] 删除后查询为 null，校验通过");
        //对从未写入未确认消息的队列调用 deleteWithAckMessage，不应抛异常（内部有 null 判断）
        Assertions.assertDoesNotThrow(() -> memoryDataCenter
                .deleteWithAckMessage("nonExistQueue", expectedMessage));
        log.info("[testAckWithMessage] 边界校验通过：删除不存在队列的 ack 消息不抛异常");
        //对从未写入未确认消息的队列调用 getWithAckMessage，应返回 null
        Message nonExist = memoryDataCenter.getWithAckMessage("nonExistQueue", "nonExistId");
        Assertions.assertNull(nonExist);
        log.info("[testAckWithMessage] 边界校验通过：查询不存在队列的 ack 消息返回 null");
        log.info("[testAckWithMessage] 未确认消息测试全部通过！");
    }

    //测试从硬盘恢复数据到内存中
    @Test
    public void testRecovery() throws IOException, MQException, ClassNotFoundException {
        //由于我们后续需要进行数据库的操作，要依赖mybatis，因此要先启动我们的SpringApplication，才可以继续拿后续的数据库操作
        MessageQueueDemoApplication.context = SpringApplication.run(MessageQueueDemoApplication.class);
        //在硬盘上构造好数据
        DiskDataCenter diskDataCenter = new DiskDataCenter();
        diskDataCenter.init();
        //往对象中插入数据
        Exchange expectedExchange = createTestExchange(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1);
        diskDataCenter.insertExchange(expectedExchange);
        MSGQueue expectedQueue = createTestQueue(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        diskDataCenter.insertQueue(expectedQueue);
        Bingding expectedBingding = new Bingding();
        expectedBingding.setExchangeName(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1);
        expectedBingding.setBindingKey(ConstantForMemoryDataCenterTest.BINGIDNG_KEY_TEST_NAME_1);
        expectedBingding.setQueueName(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        diskDataCenter.insertBingding(expectedBingding);
        //构造消息
        Message expectedMessage = createMessage(ConstantForMemoryDataCenterTest.MESSAGE_CONTENT_TEST_1);
        diskDataCenter.insertMessage(expectedQueue,expectedMessage);
        log.info("[testRecovery] 硬盘数据准备完成：1 交换机 / 1 队列 / 1 绑定 / 1 消息");
        //恢复操作
        memoryDataCenter.recovery(diskDataCenter);
        log.info("[testRecovery] recovery() 执行完毕，开始校验内存中的数据");
        //从内存中读取
        Exchange actualExchange = memoryDataCenter.getExchange(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1);
        //注意我们不能直接比较引用，因为我们把对象写入了硬盘又拿了出来
        Assertions.assertNotNull(actualExchange);
        Assertions.assertEquals(expectedExchange.getName(),actualExchange.getName());
        Assertions.assertEquals(expectedExchange.getExchangeType(),actualExchange.getExchangeType());
        Assertions.assertEquals(expectedExchange.getIsDelete(),actualExchange.getIsDelete());
        Assertions.assertEquals(expectedExchange.getIsPermanent(),actualExchange.getIsPermanent());
        log.info("[testRecovery] 交换机校验通过：name={}", actualExchange.getName());
        //针对队列对比
        MSGQueue actualQueue = memoryDataCenter.getQueue(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        Assertions.assertNotNull(actualQueue);
        Assertions.assertEquals(expectedQueue.getName(),actualQueue.getName());
        Assertions.assertEquals(expectedQueue.getIsPermanent(),actualQueue.getIsPermanent());
        Assertions.assertEquals(expectedQueue.getIsDelete(),actualQueue.getIsDelete());
        Assertions.assertEquals(expectedQueue.getExclusivel(),actualQueue.getExclusivel());
        log.info("[testRecovery] 队列校验通过：name={}", actualQueue.getName());
        //针对绑定关系的对比
        Bingding acutalBingding = memoryDataCenter
                .getBingdingOnce(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1,ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        Assertions.assertNotNull(acutalBingding);
        Assertions.assertEquals(expectedBingding.getExchangeName(),acutalBingding.getExchangeName());
        Assertions.assertEquals(expectedBingding.getQueueName(),acutalBingding.getQueueName());
        Assertions.assertEquals(expectedBingding.getBindingKey(),acutalBingding.getBindingKey());
        log.info("[testRecovery] 绑定关系校验通过：{} -> {}", acutalBingding.getExchangeName(), acutalBingding.getQueueName());
        //对比消息
        Message acutalMessage = memoryDataCenter.getMessage(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1);
        Assertions.assertNotNull(acutalMessage);
        Assertions.assertEquals(expectedMessage.getMessageId(),acutalMessage.getMessageId());
        Assertions.assertEquals(expectedMessage.getRoutingKey(),acutalMessage.getRoutingKey());
        Assertions.assertEquals(expectedMessage.getDeliverMode(),acutalMessage.getDeliverMode());
        Assertions.assertArrayEquals(expectedMessage.getBody(),acutalMessage.getBody());
        log.info("[testRecovery] 消息校验通过：messageId={}, body={}",
                acutalMessage.getMessageId(), new String(acutalMessage.getBody()));
        //recovery 后再次调用 recovery，内存应该被清空后重新填充（幂等）
        //二次 recovery 后数据仍然存在且和之前完全一致
        memoryDataCenter.recovery(diskDataCenter);
        Assertions.assertNotNull(memoryDataCenter.getExchange(ConstantForMemoryDataCenterTest.EXCHANGE_TEST_NAME_1));
        Assertions.assertNotNull(memoryDataCenter.getQueue(ConstantForMemoryDataCenterTest.QUEUE_TEST_NAME_1));
        log.info("[testRecovery] 边界校验通过：重复调用 recovery 是幂等的，数据仍然完整");
        //关闭应用程序，释放数据库连接！！
        MessageQueueDemoApplication.context.close();
        //清理硬盘数据，删除data目录的内容（递归删除）
        File dataDir = new File("./data");
        FileUtils.deleteDirectory(dataDir);
        log.info("[testRecovery] 硬盘数据清理完成，测试全部通过！");
    }
}
