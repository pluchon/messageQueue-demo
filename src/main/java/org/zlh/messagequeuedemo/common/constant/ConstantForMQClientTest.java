package org.zlh.messagequeuedemo.common.constant;

/**
 * @author pluchon
 * @create 2026-03-31-17:08
 * 作者代码水平一般，难免难看，请见谅
 */
//客户端测试常量类集合
public class ConstantForMQClientTest {
    public static final int port = 9090;

    public static final String VIRTUAL_HOST_TEST_NAME_1 = "virtual_host_test_name_1";

    public static final String EXCHANGE_TEST_NAME_1 = "exchange_test_name_1";
    public static final String EXCHANGE_NOT_EXIST = "exchange_not_exist";

    public static final String QUEUE_TEST_NAME_1 = "queue_test_name_1";
    public static final String QUEUE_TEST_NAME_2 = "queue_test_name_2";
    public static final String QUEUE_NOT_EXIST = "queue_not_exist";

    public static final String BINGDING_KEY_TEST_1 = "bingding_key_teest_1";
    public static final String BINGDING_KEY_FOR_TOPIC_TEST_1 = "aaa.*.bbb";
    // Topic 不匹配的 bindingKey，用于验证路由过滤
    public static final String BINGDING_KEY_FOR_TOPIC_NO_MATCH = "xxx.yyy.zzz";

    public static final String ROUTING_KEY_FOR_TOPIC_TEST_1 = "aaa.ccc.bbb";
    // 非法的 routingKey（含特殊字符）
    public static final String ROUTING_KEY_INVALID = "aaa@bbb!ccc";

    public static final String MESSAGE_CONTENT_TEST_1 = "test_content_1";
    public static final String MESSAGE_CONTENT_TEST_2 = "test_content_2";

    public static final String CONSUMER_TAG_TEST_1 = "consumer_tag_test_1";
    public static final String CONSUMER_TAG_TEST_2 = "consumer_tag_test_2";

    // 不存在的消息ID
    public static final String MESSAGE_ID_NOT_EXIST = "M-00000000-0000-0000-0000-000000000000";
}
