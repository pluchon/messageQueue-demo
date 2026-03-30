package org.zlh.messagequeuedemo.common.consumer;

import org.zlh.messagequeuedemo.mqserver.core.BasicProperties;

/**
 * @author pluchon
 * @create 2026-03-30-08:40
 * 作者代码水平一般，难免难看，请见谅
 */
//函数是接口，只能定义一个方法（回调函数）
@FunctionalInterface
public interface Consumer {
    //处理消息投递，即在每次收到消息后被调用
    //当我们的消费者消费到消息之后，通过参数可以进行传递，从而处理此回调函数
    void handleDelivery(String consumerType, BasicProperties basicProperties,byte[] body);
}
