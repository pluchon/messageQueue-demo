package org.zlh.messagequeuedemo.mqserver.core;

import lombok.Data;

/**
 * @author pluchon
 * @create 2026-03-25-15:52
 * 作者代码水平一般，难免难看，请见谅
 */
//我们定义一个绑定类型，表示我们交换机和队列之间的交换关系
// 我们一个bingding只能表示一个和一个的绑定关系
@Data
public class Bingding {
    // 因为我们交换机和队列都是用name作为身份标识
    private String exchangeName;
    private String queueName;
    // bingkey表示我们主题交换机，是一个暗号的口令
    // 后续我们发的消息有个routingKey，要和我们的bindingKey进行校验
    private String bindingKey;

    // 为什么我们不持久化，如果我们的交换机或者是队列有一个没有持久化，那我们当前绑定持久化是没有任何意义的......
}