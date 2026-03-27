package org.zlh.messagequeuedemo.mqserver.datacenter;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.zlh.messagequeuedemo.MessageQueueDemoApplication;
import org.zlh.messagequeuedemo.common.constant.ConstantForDateBaseTest;
import org.zlh.messagequeuedemo.mqserver.core.Bingding;
import org.zlh.messagequeuedemo.mqserver.core.Exchange;
import org.zlh.messagequeuedemo.mqserver.core.ExchangeTtype;
import org.zlh.messagequeuedemo.mqserver.core.MSGQueue;

import java.util.List;

/**
 * @author pluchon
 * @create 2026-03-26-09:23
 *         作者代码水平一般，难免难看，请见谅
 */
// 单元测试就是保证当前测试的类不收任何干扰
@SpringBootTest
@Slf4j
class DataBaseManagerTest {
    // 准备好对应类
    private final DataBaseManager dataBaseManager = new DataBaseManager();

    // 每个用例执行前都调用这个方法
    @BeforeEach
    public void setUp() {
        // 生成对象，因为我们的init中要拿到metaMapper实例对象，因此要先获取上下文内容
        MessageQueueDemoApplication.context = SpringApplication.run(MessageQueueDemoApplication.class);
        dataBaseManager.init();
    }

    // 每个用例执行后都调用这个方法
    @AfterEach
    public void tearDown() {
        // 删除数据库，注意要先关闭context上下文，因为我们context已经持有了DataBaseManager实例
        // 因此，如果DataBaseManager被别人打开使用了就无法删除（Windows），并且释放我们的8080端口
        MessageQueueDemoApplication.context.close();
        dataBaseManager.deleteDB();
    }

    // 交换机构建操作
    private Exchange createExchange(String exchangeName) {
        Exchange newExchange = new Exchange();
        newExchange.setName(exchangeName);
        newExchange.setExchangeType(ExchangeTtype.FINOUT);
        newExchange.setIsDelete(false);
        newExchange.setIsPermanent(true);
        // 使用常量替换魔法字符串
        newExchange.setArgumentNOJSON(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_KEY_1, ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_VALUE_1);
        newExchange.setArgumentNOJSON(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_KEY_2, ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_VALUE_2);
        return newExchange;
    }

    //创建队列
    private MSGQueue createQueue(String queueName){
        MSGQueue newQueue = new MSGQueue();
        newQueue.setName(queueName);
        newQueue.setIsDelete(false);
        newQueue.setIsPermanent(true);
        newQueue.setExclusivel(false);
        newQueue.setArgumentsNOJSON(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_KEY_1, ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_VALUE_1);
        newQueue.setArgumentsNOJSON(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_KEY_2, ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_VALUE_2);
        return newQueue;
    }

    //创建绑定关系（需要先有交换机和队列才能绑定）
    private Bingding createBingding(String exchangeName, String queueName, String bindingKey) {
        Bingding bingding = new Bingding();
        bingding.setExchangeName(exchangeName);
        bingding.setQueueName(queueName);
        bingding.setBindingKey(bindingKey);
        return bingding;
    }

    // 初始化数据库方法测试
    @Test
    public void testInit() {
        List<Exchange> exchangeList = dataBaseManager.queryAllExchange();
        List<MSGQueue> msgQueueList = dataBaseManager.queryAllQueue();
        List<Bingding> bingdingList = dataBaseManager.queryAllBingding();
        //初始状态只有一个默认匿名交换机，队列和绑定都为空
        Assertions.assertEquals(1, exchangeList.size());
        Assertions.assertEquals(0, msgQueueList.size());
        Assertions.assertEquals(0, bingdingList.size());
        //校验默认交换机的具体内容
        Exchange defaultExchange = exchangeList.get(0);
        Assertions.assertEquals("", defaultExchange.getName());
        Assertions.assertEquals(ExchangeTtype.DIRECT, defaultExchange.getExchangeType());
        //默认交换机不能是 null
        Assertions.assertNotNull(defaultExchange);
        //默认交换机应该是持久化的
        Assertions.assertTrue(defaultExchange.getIsPermanent());
        //默认交换机不应该被标记为删除
        Assertions.assertFalse(defaultExchange.getIsDelete());
        log.info("初始化方法校验成功！");
    }

    // 交换机插入
    @Test
    public void testInsertExchange() {
        Exchange newExchange = createExchange(ConstantForDateBaseTest.TEST_INSERT_EXCHANGE_NAME_1);
        dataBaseManager.insertExchange(newExchange);
        List<Exchange> exchangeList = dataBaseManager.queryAllExchange();
        //插入后应有 2 条（默认 + 新插入）
        Assertions.assertEquals(2, exchangeList.size());
        Exchange newInsertExchange = exchangeList.get(1);
        // 校验各字段
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_INSERT_EXCHANGE_NAME_1, newInsertExchange.getName());
        Assertions.assertEquals(ExchangeTtype.FINOUT, newInsertExchange.getExchangeType());
        Assertions.assertFalse(newInsertExchange.getIsDelete());
        Assertions.assertTrue(newInsertExchange.getIsPermanent());
        // 校验argument参数
        Assertions.assertEquals(2, newInsertExchange.getArgumentMap().size());
        Assertions.assertEquals(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_VALUE_1,
                newInsertExchange.getArgumentNOJSON(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_KEY_1));
        Assertions.assertEquals(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_VALUE_2,
                newInsertExchange.getArgumentNOJSON(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_KEY_2));
        //查询不存在的 key 应该返回 null，不能抛异常
        Assertions.assertNull(newInsertExchange.getArgumentNOJSON("nonExistKey"));
        //对象本身不为 null
        Assertions.assertNotNull(newInsertExchange);
        log.info("交换机插入校验成功！");
    }

    // 交换机删除
    @Test
    public void testDeleteExchange() {
        Exchange newExchange = createExchange(ConstantForDateBaseTest.TEST_DELETE_EXCHANGE_NAME_1);
        dataBaseManager.insertExchange(newExchange);
        // 插入后验证存在
        List<Exchange> exchangeList = dataBaseManager.queryAllExchange();
        Assertions.assertEquals(2, exchangeList.size());
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_DELETE_EXCHANGE_NAME_1, exchangeList.get(1).getName());
        // 执行删除
        dataBaseManager.deleteExchange(ConstantForDateBaseTest.TEST_DELETE_EXCHANGE_NAME_1);
        // 删除后验证：只剩默认交换机
        exchangeList = dataBaseManager.queryAllExchange();
        Assertions.assertEquals(1, exchangeList.size());
        Assertions.assertEquals("", exchangeList.get(0).getName());
        //删除一个不存在的交换机名，不应该抛异常
        Assertions.assertDoesNotThrow(() -> dataBaseManager.deleteExchange("nonExistExchange"));
        //删除不存在的交换机后，数量仍然不变（还是 1）
        exchangeList = dataBaseManager.queryAllExchange();
        Assertions.assertEquals(1, exchangeList.size());
        log.info("交换机删除校验成功！");
    }

    // 队列插入
    @Test
    public void testInsertQueue() {
        MSGQueue newInsertQueue = createQueue(ConstantForDateBaseTest.TEST_INSERT_QUEUE_NAME_1);
        dataBaseManager.insertQueue(newInsertQueue);
        List<MSGQueue> msgQueueList = dataBaseManager.queryAllQueue();
        //注意我们最开始没有任何队列数据，我们插入后只有一条队列数据，这个和之前的Exchange交换机不一样
        Assertions.assertEquals(1, msgQueueList.size());
        MSGQueue newQueue = msgQueueList.get(0);
        //校验各字段
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_INSERT_QUEUE_NAME_1, newQueue.getName());
        Assertions.assertTrue(newQueue.getIsPermanent());
        Assertions.assertFalse(newQueue.getIsDelete());
        Assertions.assertFalse(newQueue.getExclusivel());
        //校验 argument 的两个 MAP 参数
        Assertions.assertEquals(2, newQueue.getArgumentsNOJSON().size());
        Assertions.assertEquals(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_VALUE_1,
                newQueue.getArgumentsNOJSON().get(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_KEY_1));
        Assertions.assertEquals(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_VALUE_2,
                newQueue.getArgumentsNOJSON().get(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_KEY_2));
        //对象本身不为 null
        Assertions.assertNotNull(newQueue);
        //查询不存在的 key 返回 null
        Assertions.assertNull(newQueue.getArgumentsNOJSON().get("nonExistKey"));
        log.info("队列插入校验成功！");
    }

    //队列删除
    @Test
    public void testDeleteQueue() {
        MSGQueue newQueue = createQueue(ConstantForDateBaseTest.TEST_DELETE_QUEUE_NAME_1);
        dataBaseManager.insertQueue(newQueue);
        //插入后验证存在
        List<MSGQueue> msgQueueList = dataBaseManager.queryAllQueue();
        Assertions.assertEquals(1, msgQueueList.size());
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_DELETE_QUEUE_NAME_1, msgQueueList.get(0).getName());
        //执行删除
        dataBaseManager.deleteQueue(ConstantForDateBaseTest.TEST_DELETE_QUEUE_NAME_1);
        //删除后验证：列表应为空
        msgQueueList = dataBaseManager.queryAllQueue();
        Assertions.assertEquals(0, msgQueueList.size());
        //删除一个不存在的队列名，不应该抛异常
        Assertions.assertDoesNotThrow(() -> dataBaseManager.deleteQueue("nonExistQueue"));
        //删除不存在的队列后数量仍然是 0
        msgQueueList = dataBaseManager.queryAllQueue();
        Assertions.assertEquals(0, msgQueueList.size());
        log.info("队列删除校验成功！");
    }

    //绑定插入
    @Test
    public void testInsertBingding() {
        //绑定需要依赖已存在的交换机与队列，先创建它们
        Exchange exchange = createExchange(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME);
        MSGQueue queue1 = createQueue(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_1);
        MSGQueue queue2 = createQueue(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_2);
        dataBaseManager.insertExchange(exchange);
        dataBaseManager.insertQueue(queue1);
        dataBaseManager.insertQueue(queue2);
        //创建两条绑定关系：同一个交换机绑定两个不同队列
        Bingding bingding1 = createBingding(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME,
                ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_1, ConstantForDateBaseTest.TEST_BINGDING_BINDING_KEY);
        Bingding bingding2 = createBingding(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME,
                ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_2, ConstantForDateBaseTest.TEST_BINGDING_BINDING_KEY);
        dataBaseManager.insertBingding(bingding1);
        dataBaseManager.insertBingding(bingding2);
        //查询验证
        List<Bingding> bingdingList = dataBaseManager.queryAllBingding();
        Assertions.assertEquals(2, bingdingList.size());
        //校验第一条绑定的具体字段
        Bingding result1 = bingdingList.get(0);
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME, result1.getExchangeName());
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_1, result1.getQueueName());
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_BINGDING_BINDING_KEY, result1.getBindingKey());
        //对象本身不为 null
        Assertions.assertNotNull(result1);
        log.info("绑定插入校验成功！");
    }

    //绑定删除
    @Test
    public void testDeleteBingding() {
        //同上，先建好交换机和队列
        Exchange exchange = createExchange(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME);
        MSGQueue queue1 = createQueue(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_1);
        MSGQueue queue2 = createQueue(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_2);
        dataBaseManager.insertExchange(exchange);
        dataBaseManager.insertQueue(queue1);
        dataBaseManager.insertQueue(queue2);
        //插入两条绑定关系
        Bingding bingding1 = createBingding(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME,
                ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_1, ConstantForDateBaseTest.TEST_BINGDING_BINDING_KEY);
        Bingding bingding2 = createBingding(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME,
                ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_2, ConstantForDateBaseTest.TEST_BINGDING_BINDING_KEY);
        dataBaseManager.insertBingding(bingding1);
        dataBaseManager.insertBingding(bingding2);
        //删除第一条绑定
        dataBaseManager.deleteBingding(bingding1);
        //验证只剩一条，且是 queue2 的那条
        List<Bingding> bingdingList = dataBaseManager.queryAllBingding();
        Assertions.assertEquals(1, bingdingList.size());
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_2, bingdingList.get(0).getQueueName());
        //再删除第二条
        dataBaseManager.deleteBingding(bingding2);
        bingdingList = dataBaseManager.queryAllBingding();
        Assertions.assertEquals(0, bingdingList.size());
        //删除不存在的绑定不应该抛异常
        Bingding nonExist = createBingding("nonExist", "nonExist", "nonExist");
        Assertions.assertDoesNotThrow(() -> dataBaseManager.deleteBingding(nonExist));
        //删除后数量仍是 0
        Assertions.assertEquals(0, dataBaseManager.queryAllBingding().size());
        log.info("绑定删除校验成功！");
    }
}

