package org.zlh.messagequeuedemo.mqclient.consumer;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author pluchon
 * @create 2026-03-30-08:55
 * 作者代码水平一般，难免难看，请见谅
 */
//消费者属性与方法
@Data
@AllArgsConstructor
public class ConsumerEnv {
    //消费者标识
    private String consumerTag;
    //消费者所要订阅的队列
    private String queueName;
    //当前这个订阅是否需要手动应答
    private boolean autoAck;
    //收到消息后回调函数来处理我们收到的消息
    private Consumer consumer;
}
