package org.zlh.messagequeuedemo.mqserver.datacenter;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;
import org.zlh.messagequeuedemo.common.constant.ConstantForMessageFileTest;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.mqserver.core.MSGQueue;
import org.zlh.messagequeuedemo.mqserver.core.Message;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author pluchon
 * @create 2026-03-27-23:13
 *         作者代码水平一般，难免难看，请见谅
 */
// 消息文件的单元测试类
@Slf4j
@SpringBootTest
public class MessageFileManagerTest {
    private MessageFileManager messageFileManager = new MessageFileManager();

    // 每个用例执行前的准备工作，创建队列的目录
    @BeforeEach
    public void setUp() throws IOException {
        // 创建出两个队列来，以备后用
        messageFileManager.createQueue(ConstantForMessageFileTest.QUEUE_NAME_1);
        messageFileManager.createQueue(ConstantForMessageFileTest.QUEUE_NAME_2);
        log.info("[setUp] 创建队列目录完成：{} / {}", ConstantForMessageFileTest.QUEUE_NAME_1, ConstantForMessageFileTest.QUEUE_NAME_2);
    }

    // 每个用例执行完毕的首位工作
    @AfterEach
    public void tearDown() throws IOException {
        // 把刚才创建的两个队列进行销毁
        messageFileManager.deleteQueue(ConstantForMessageFileTest.QUEUE_NAME_1);
        messageFileManager.deleteQueue(ConstantForMessageFileTest.QUEUE_NAME_2);
        log.info("[tearDown] 队列目录清理完成");
    }

    // 创建文件，我们已经在setUp测试过了，我们仅需检测是否存在就好了（队列以及其附属的统计文件）
    @Test
    public void testCreateFile() {
        File queueFile1 = new File("./data/" + ConstantForMessageFileTest.QUEUE_NAME_1 + "/queue_data.txt");
        Assertions.assertTrue(queueFile1.isFile());
        File queueFileStat1 = new File("./data/" + ConstantForMessageFileTest.QUEUE_NAME_1 + "/queue_stat.txt");
        Assertions.assertTrue(queueFileStat1.isFile());
        File queueFile2 = new File("./data/" + ConstantForMessageFileTest.QUEUE_NAME_2 + "/queue_data.txt");
        Assertions.assertTrue(queueFile2.isFile());
        File queueFileStat2 = new File("./data/" + ConstantForMessageFileTest.QUEUE_NAME_2 + "/queue_stat.txt");
        Assertions.assertTrue(queueFileStat2.isFile());
        log.info("[testCreateFile] 队列1数据文件路径：{}", queueFile1.getAbsolutePath());
        log.info("[testCreateFile] 队列1统计文件路径：{}", queueFileStat1.getAbsolutePath());
        log.info("[testCreateFile] 队列2数据文件路径：{}", queueFile2.getAbsolutePath());
        log.info("[testCreateFile] 队列2统计文件路径：{}", queueFileStat2.getAbsolutePath());
        //重复创建同一个队列目录，不应该抛异常
        Assertions.assertDoesNotThrow(() -> messageFileManager.createQueue(ConstantForMessageFileTest.QUEUE_NAME_1));
        //重复创建后文件依然存在（没有被覆盖清空）
        Assertions.assertTrue(queueFile1.isFile());
        log.info("[testCreateFile] 重复创建校验通过，文件校验成功！");
    }

    // 测试我们的消息统计文件
    @Test
    public void testReadWriteStat() {
        MessageFileManager.Stat stat = new MessageFileManager.Stat();
        stat.validCount = 20;
        stat.totalCount = 100;
        log.info("[testReadWriteStat] 写入 stat -> totalCount={}, validCount={}", stat.totalCount, stat.validCount);
        // 因为是private方法，无法直接调用，我们使用反射！！
        // JAVA原有的反射非常难用，因此我们引入Spring的工具类
        // 调用的对象的实例，方法名，这个方法对应的参数
        ReflectionTestUtils.invokeMethod(messageFileManager, "setStat", ConstantForMessageFileTest.QUEUE_NAME_1, stat);
        // 读取我们的统计数据
        MessageFileManager.Stat newStat = ReflectionTestUtils
                .invokeMethod(messageFileManager, "getStat", ConstantForMessageFileTest.QUEUE_NAME_1);
        // 和之前进行比较
        Assertions.assertEquals(stat.totalCount, newStat.totalCount);
        Assertions.assertEquals(stat.validCount, newStat.validCount);
        log.info("[testReadWriteStat] 读出 stat -> totalCount={}, validCount={}", newStat.totalCount, newStat.validCount);
        //连续写两次，以第二次写入的结果为准（覆盖写，非追加）
        MessageFileManager.Stat stat2 = new MessageFileManager.Stat();
        stat2.validCount = 5;
        stat2.totalCount = 50;
        ReflectionTestUtils.invokeMethod(messageFileManager, "setStat", ConstantForMessageFileTest.QUEUE_NAME_1, stat2);
        MessageFileManager.Stat newStat2 = ReflectionTestUtils
                .invokeMethod(messageFileManager, "getStat", ConstantForMessageFileTest.QUEUE_NAME_1);
        Assertions.assertEquals(50, newStat2.totalCount);
        Assertions.assertEquals(5, newStat2.validCount);
        log.info("[testReadWriteStat] 覆盖写校验通过：totalCount={}, validCount={}", newStat2.totalCount, newStat2.validCount);
        log.info("[testReadWriteStat] 统计文件读写校验成功！");
    }

    // 构建队列
    private MSGQueue createQueue(String queueName) {
        MSGQueue msgQueue = new MSGQueue();
        msgQueue.setName(queueName);
        msgQueue.setIsPermanent(true);
        msgQueue.setIsDelete(false);
        msgQueue.setExclusivel(false);
        return msgQueue;
    }

    // 构建消息
    private Message createMessage(String content) {
        // 调用工厂方法
        return Message.messageCreateWithIDFactory(ConstantForMessageFileTest.ROUTING_KEY_1, null, content.getBytes());
    }

    // 测试发送消息
    @Test
    public void testSendMessage() throws MQException, IOException, ClassNotFoundException {
        // 构造消息与队列
        Message message = createMessage(ConstantForMessageFileTest.MESSAGE_CONTENT_1);
        // 创建队列名要和我们之前创建的一致，因为要保证我们的对应目录已经文件都存在
        MSGQueue queue = createQueue(ConstantForMessageFileTest.QUEUE_NAME_1);
        log.info("[testSendMessage] 发送消息 -> messageId={}, routingKey={}, body={}",
                message.getMessageId(), message.getRoutingKey(), new String(message.getBody()));
        // 发送消息
        messageFileManager.sendMessage(queue, message);
        // 检查统计文件
        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(messageFileManager, "getStat",
                ConstantForMessageFileTest.QUEUE_NAME_1);
        // 验证结果
        Assertions.assertEquals(1, stat.validCount);
        Assertions.assertEquals(1, stat.totalCount);
        log.info("[testSendMessage] 统计文件校验通过：totalCount={}, validCount={}", stat.totalCount, stat.validCount);
        // 检查消息数据文件
        LinkedList<Message> messageLinkedList = messageFileManager
                .queryAllMessage(ConstantForMessageFileTest.QUEUE_NAME_1);
        Assertions.assertEquals(1, messageLinkedList.size());
        Assertions.assertEquals(message.getMessageId(), messageLinkedList.get(0).getMessageId());
        Assertions.assertEquals(message.getRoutingKey(), messageLinkedList.get(0).getRoutingKey());
        Assertions.assertEquals(message.getDeliverMode(), messageLinkedList.get(0).getDeliverMode());
        // 注意不能直接使用assertEquals，因为我们比较的是数组
        Assertions.assertArrayEquals(message.getBody(), messageLinkedList.get(0).getBody());
        log.info("[testSendMessage] 读回消息校验通过 -> messageId={}, body={}",
                messageLinkedList.get(0).getMessageId(), new String(messageLinkedList.get(0).getBody()));
        //往不存在的队列发送消息，应该抛出 MQException
        MSGQueue nonExistQueue = createQueue("nonExistQueue");
        Assertions.assertThrows(MQException.class, () -> messageFileManager.sendMessage(nonExistQueue, message));
        log.info("[testSendMessage] 边界校验通过：往不存在队列发消息抛出 MQException");
        log.info("[testSendMessage] 发送消息校验成功！");
    }

    // 测试加载所有消息，虽然之前测试过了，但是为了测试，我们这里多搞几个消息来测试
    // 我们插入100条消息，读取后看看是不是和我们创建的100条消息对应
    @Test
    public void testQueryAllMessage() throws MQException, IOException, ClassNotFoundException {
        MSGQueue queue = createQueue(ConstantForMessageFileTest.QUEUE_NAME_1);
        List<Message> expectedMessageList = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message messageInfo = null;
            if (i % 3 == 0) {
                messageInfo = createMessage(ConstantForMessageFileTest.MESSAGE_CONTENT_1 + i);
            } else if (i % 3 == 1) {
                messageInfo = createMessage(ConstantForMessageFileTest.MESSAGE_CONTENT_2 + i);
            } else {
                messageInfo = createMessage(ConstantForMessageFileTest.MESSAGE_CONTENT_3 + i);
            }
            messageFileManager.sendMessage(queue, messageInfo);
            expectedMessageList.add(messageInfo);
        }
        log.info("[testQueryAllMessage] 已写入 100 条消息");
        //读取所有的消息
        LinkedList<Message> messageLinkedList = messageFileManager.queryAllMessage(ConstantForMessageFileTest.QUEUE_NAME_1);
        //先校验总数量
        Assertions.assertEquals(expectedMessageList.size(), messageLinkedList.size());
        log.info("[testQueryAllMessage] 总数量校验通过：{} 条", messageLinkedList.size());
        //检查统计文件和实际数据的总数是否匹配
        MessageFileManager.Stat stat = ReflectionTestUtils.
                invokeMethod(messageFileManager, "getStat", ConstantForMessageFileTest.QUEUE_NAME_1);
        Assertions.assertEquals(100, stat.totalCount);
        Assertions.assertEquals(100, stat.validCount);
        log.info("[testQueryAllMessage] 统计文件校验通过：totalCount={}, validCount={}", stat.totalCount, stat.validCount);
        //保证每一个消息一致性
        for (int i = 0; i < expectedMessageList.size(); i++) {
            Message expectedMessage = expectedMessageList.get(i);
            Message message = messageLinkedList.get(i);
            //对比各字段，和testSendMessage保持一致
            Assertions.assertEquals(expectedMessage.getMessageId(), message.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), message.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliverMode(), message.getDeliverMode());
            //body是字节数组，不能直接用assertEquals
            Assertions.assertArrayEquals(expectedMessage.getBody(), message.getBody());
            if (i % 3 == 0) {
                Assertions.assertArrayEquals((ConstantForMessageFileTest.MESSAGE_CONTENT_1 + i).getBytes(), message.getBody());
            } else if (i % 3 == 1) {
                Assertions.assertArrayEquals((ConstantForMessageFileTest.MESSAGE_CONTENT_2 + i).getBytes(), message.getBody());
            } else {
                Assertions.assertArrayEquals((ConstantForMessageFileTest.MESSAGE_CONTENT_3 + i).getBytes(), message.getBody());
            }
        }
        log.info("[testQueryAllMessage] 逐条消息内容校验通过，共 {} 条", expectedMessageList.size());
        //队列2没有写入任何消息，读取结果应该是空列表，而不是 null
        LinkedList<Message> emptyList = messageFileManager.queryAllMessage(ConstantForMessageFileTest.QUEUE_NAME_2);
        Assertions.assertNotNull(emptyList);
        Assertions.assertEquals(0, emptyList.size());
        log.info("[testQueryAllMessage] 边界校验通过：空队列读取返回空列表，非 null");
        log.info("[testQueryAllMessage] 所有消息读取校验成功！");
    }

    //删除消息测试
    //创建一个队列，写入10个消息，删除其中几个，再读取所有消息，看是否符合预期
    @Test
    public void testDeleteMessage() throws MQException, IOException, ClassNotFoundException {
        MSGQueue queue = createQueue(ConstantForMessageFileTest.QUEUE_NAME_1);
        List<Message> expectedMessageList = new LinkedList<>();
        for(int i = 0;i < 10;i++){
            Message messageInfo = null;
            if (i % 3 == 0) {
                messageInfo = createMessage(ConstantForMessageFileTest.MESSAGE_CONTENT_1 + i);
            } else if (i % 3 == 1) {
                messageInfo = createMessage(ConstantForMessageFileTest.MESSAGE_CONTENT_2 + i);
            } else {
                messageInfo = createMessage(ConstantForMessageFileTest.MESSAGE_CONTENT_3 + i);
            }
            messageFileManager.sendMessage(queue, messageInfo);
            expectedMessageList.add(messageInfo);
        }
        log.info("[testDeleteMessage] 已写入 10 条消息");
        //删除最后三个
        messageFileManager.deleteMessage(queue,expectedMessageList.get(7));
        messageFileManager.deleteMessage(queue,expectedMessageList.get(8));
        messageFileManager.deleteMessage(queue,expectedMessageList.get(9));
        log.info("[testDeleteMessage] 已软删除下标 7/8/9 的消息");
        //对比内容是否符合了要求
        LinkedList<Message> messageLinkedList = messageFileManager.queryAllMessage(ConstantForMessageFileTest.QUEUE_NAME_1);
        Assertions.assertEquals(7,messageLinkedList.size());
        log.info("[testDeleteMessage] 删除后有效消息数量校验通过：{} 条", messageLinkedList.size());
        //检查统计文件：totalCount 仍是 10（软删除不减总数），validCount 应该是 7
        MessageFileManager.Stat stat = ReflectionTestUtils
                .invokeMethod(messageFileManager, "getStat", ConstantForMessageFileTest.QUEUE_NAME_1);
        Assertions.assertEquals(10, stat.totalCount);
        Assertions.assertEquals(7, stat.validCount);
        log.info("[testDeleteMessage] 统计文件校验通过：totalCount={}, validCount={}", stat.totalCount, stat.validCount);
        //消息是否符合要求
        for(int i = 0;i < messageLinkedList.size();i++){
            Message expectedMessage = expectedMessageList.get(i);
            Message message = messageLinkedList.get(i);
            //对比各字段，和testSendMessage保持一致
            Assertions.assertEquals(expectedMessage.getMessageId(), message.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), message.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliverMode(), message.getDeliverMode());
            //body是字节数组，不能直接用assertEquals
            Assertions.assertArrayEquals(expectedMessage.getBody(), message.getBody());
            if (i % 3 == 0) {
                Assertions.assertArrayEquals((ConstantForMessageFileTest.MESSAGE_CONTENT_1 + i).getBytes(), message.getBody());
            } else if (i % 3 == 1) {
                Assertions.assertArrayEquals((ConstantForMessageFileTest.MESSAGE_CONTENT_2 + i).getBytes(), message.getBody());
            } else {
                Assertions.assertArrayEquals((ConstantForMessageFileTest.MESSAGE_CONTENT_3 + i).getBytes(), message.getBody());
            }
        }
        log.info("[testDeleteMessage] 逐条内容校验通过：{} 条有效消息均正确", messageLinkedList.size());
        log.info("[testDeleteMessage] 删除消息校验成功！");
    }

    //测试垃圾回收
    //先在队列中写100个消息，再把100个消息的一半都删除掉（下标为偶数的删除），并获取到我们的文件大小
    //手动调用GC，得到的新文件大小是否比之前小了
    @Test
    public void testGC() throws MQException, IOException, ClassNotFoundException {
        MSGQueue queue = createQueue(ConstantForMessageFileTest.QUEUE_NAME_1);
        List<Message> expectedMessageList = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message messageInfo = null;
            if (i % 3 == 0) {
                messageInfo = createMessage(ConstantForMessageFileTest.MESSAGE_CONTENT_1 + i);
            } else if (i % 3 == 1) {
                messageInfo = createMessage(ConstantForMessageFileTest.MESSAGE_CONTENT_2 + i);
            } else {
                messageInfo = createMessage(ConstantForMessageFileTest.MESSAGE_CONTENT_3 + i);
            }
            messageFileManager.sendMessage(queue, messageInfo);
            expectedMessageList.add(messageInfo);
        }
        log.info("[testGC] 已写入 100 条消息");
        //获取GC前的文件大小
        File beforeGC = new File("./data/"+ConstantForMessageFileTest.QUEUE_NAME_1+"/queue_data.txt");
        long beforeGCLength = beforeGC.length();
        log.info("[testGC] GC 前文件大小：{} bytes", beforeGCLength);
        //删除偶数下标的消息
        for(int i = 0;i < 100;i += 2){
            messageFileManager.deleteMessage(queue,expectedMessageList.get(i));
        }
        log.info("[testGC] 已软删除 50 条偶数下标消息");
        //边界：GC 前先检查 checkGC 的返回值是否符合预期（50/100 < 50%，应该触发 GC 条件）
        //注意我们触发条件是 totalCount > 2000，这里 100 条不满足，checkGC 应该返回 false
        boolean shouldGC = messageFileManager.checkGC(ConstantForMessageFileTest.QUEUE_NAME_1);
        Assertions.assertFalse(shouldGC);
        log.info("[testGC] checkGC 边界校验通过：总数 100 < 2000，不触发自动 GC，结果={}", shouldGC);
        //手动调用GC
        messageFileManager.GC(queue);
        log.info("[testGC] GC 执行完毕");
        //重新读取消息队列
        List<Message> messageList = messageFileManager.queryAllMessage(ConstantForMessageFileTest.QUEUE_NAME_1);
        Assertions.assertEquals(50,messageList.size());
        log.info("[testGC] GC 后有效消息数量校验通过：{} 条", messageList.size());
        //GC 后统计文件中 totalCount 和 validCount 应该同步更新为 50
        MessageFileManager.Stat statAfterGC = ReflectionTestUtils
                .invokeMethod(messageFileManager, "getStat", ConstantForMessageFileTest.QUEUE_NAME_1);
        Assertions.assertEquals(50, statAfterGC.totalCount);
        Assertions.assertEquals(50, statAfterGC.validCount);
        log.info("[testGC] GC 后统计文件校验通过：totalCount={}, validCount={}", statAfterGC.totalCount, statAfterGC.validCount);
        //取出每一个消息对比，注意我们删除了偶数下标元素，因此我们对比的要是奇数下标的消息
        for(int i = 0;i < messageList.size();i++){
            //GC后存活的是奇数下标消息，原始下标 = 2*i+1
            int originalIndex = 2 * i + 1;
            Message expectedMessage = expectedMessageList.get(originalIndex);
            Message message = messageList.get(i);
            //对比各字段，和testSendMessage保持一致
            Assertions.assertEquals(expectedMessage.getMessageId(), message.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), message.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliverMode(), message.getDeliverMode());
            //body是字节数组，不能直接用assertEquals
            Assertions.assertArrayEquals(expectedMessage.getBody(), message.getBody());
            //内容比对要用原始下标，因为消息内容是按原始下标决定的，而不是循环变量i
            if (originalIndex % 3 == 0) {
                Assertions.assertArrayEquals((ConstantForMessageFileTest.MESSAGE_CONTENT_1 + originalIndex).getBytes(), message.getBody());
            } else if (originalIndex % 3 == 1) {
                Assertions.assertArrayEquals((ConstantForMessageFileTest.MESSAGE_CONTENT_2 + originalIndex).getBytes(), message.getBody());
            } else {
                Assertions.assertArrayEquals((ConstantForMessageFileTest.MESSAGE_CONTENT_3 + originalIndex).getBytes(), message.getBody());
            }
        }
        log.info("[testGC] 逐条消息内容校验通过：{} 条奇数下标消息均正确", messageList.size());
        //重新读取消息数据文件大小
        File afterGC = new File("./data/"+ConstantForMessageFileTest.QUEUE_NAME_1+"/queue_data.txt");
        long afterGCLength = afterGC.length();
        log.info("[testGC] GC 前文件大小：{} bytes，GC 后文件大小：{} bytes，节省：{} bytes",
                beforeGCLength, afterGCLength, beforeGCLength - afterGCLength);
        //比较结果
        Assertions.assertTrue(beforeGCLength > afterGCLength);
        log.info("[testGC] GC 文件大小校验通过，垃圾回收校验成功！");
    }
}
