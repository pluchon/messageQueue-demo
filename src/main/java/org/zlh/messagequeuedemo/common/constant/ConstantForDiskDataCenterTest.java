package org.zlh.messagequeuedemo.common.constant;

/**
 * @author pluchon
 * @create 2026-03-28-10:48
 *         作者代码水平一般，难免难看，请见谅
 */
//常量类，专门针对于 DiskDataCenter 测试
public class ConstantForDiskDataCenterTest {
    /*
    交换机模块
     */
    public static final String EXCHANGE_NAME_1 = "disk_test_exchange1";
    public static final String EXCHANGE_NAME_2 = "disk_test_exchange2";

    /*
    队列模块
     */
    public static final String QUEUE_NAME_1 = "disk_test_queue1";
    public static final String QUEUE_NAME_2 = "disk_test_queue2";

    /*
    bindingKey 模块
     */
    public static final String BINDING_KEY_1 = "disk_test_binding_key1";

    /*
    routingKey 模块
     */
    public static final String ROUTING_KEY_1 = "disk_test_routing_key1";

    /*
    消息内容模块
     */
    public static final String MESSAGE_CONTENT_1 = "disk_test_message_content1_";
    public static final String MESSAGE_CONTENT_2 = "disk_test_message_content2_";
    public static final String MESSAGE_CONTENT_3 = "disk_test_message_content3_";
}
