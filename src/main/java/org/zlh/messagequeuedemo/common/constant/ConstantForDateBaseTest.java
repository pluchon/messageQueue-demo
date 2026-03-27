package org.zlh.messagequeuedemo.common.constant;

/**
 * @author pluchon
 * @create 2026-03-26-10:24
 * 作者代码水平一般，难免难看，请见谅
 */
//常量类，定义一些常量，专门针对于数据库管理
public class ConstantForDateBaseTest {
    /*
    交换机模块
     */
    public static final String CREATE_EXCHANGE_ARGUMENT_KEY_1 = "simple1";
    public static final String CREATE_EXCHANGE_ARGUMENT_VALUE_1 = "hello exchange!";
    public static final String CREATE_EXCHANGE_ARGUMENT_KEY_2 = "simple2";
    public static final String CREATE_EXCHANGE_ARGUMENT_VALUE_2 = "hi exchange!";

    public static final String TEST_INSERT_EXCHANGE_NAME_1 = "simpleExchangeNameForTest";
    public static final String TEST_DELETE_EXCHANGE_NAME_1 = "simpleExchangeNameForDelete";

    /*
    队列模块
     */
    public static final String CREATE_QUEUE_ARGUMENT_KEY_1 = "simple1";
    public static final String CREATE_QUEUE_ARGUMENT_VALUE_1 = "hello queue!";
    public static final String CREATE_QUEUE_ARGUMENT_KEY_2 = "simple2";
    public static final String CREATE_QUEUE_ARGUMENT_VALUE_2 = "hi queue!";

    public static final String TEST_INSERT_QUEUE_NAME_1 = "simpleQueueNameForTest";
    public static final String TEST_DELETE_QUEUE_NAME_1 = "simpleQueueNameForDelete";

    /*
    绑定关系模块
     */
    public static final String TEST_BINGDING_EXCHANGE_NAME = "simpleExchangeForBingding";
    public static final String TEST_BINGDING_QUEUE_NAME_1  = "simpleQueueForBingding1";
    public static final String TEST_BINGDING_QUEUE_NAME_2  = "simpleQueueForBingding2";
    public static final String TEST_BINGDING_BINDING_KEY   = "testBindingKey";
}
