package org.zlh.messagequeuedemo.mqserver.core;

import lombok.Data;

import java.io.Serializable;

/**
 * @author pluchon
 * @create 2026-03-25-17:16
 * 作者代码水平一般，难免难看，请见谅
 */
//消息的属性
@Data
public class BasicProperties implements Serializable {
    // 消息的唯一的身份标识，我们使用UUID表示我们的ID唯一性
    private String messageID;
    // 如果交换机是直接交换机->我们表示我们要转发的队列名字
    // 如果是删除交换机->我们忽略
    // 如果是主题交换机，要和我们的主题交换机的BingKey一一对应
    private String routingKey;
    //deliverMode表示我们消息是否要进行持久化
    // 1->不，2->持久化
    // 不使用枚举类，因为我们的rabbitmq也是这么设计的
    private Integer deliverMode;
    // TODO 针对我们的rabbitmq来说，还是有很多别的属性的，我们可以留作我们后续进行扩展
}
