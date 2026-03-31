package org.zlh.messagequeuedemo.mqclient.dto.arguments.virtualhost;

import lombok.Getter;
import lombok.Setter;
import org.zlh.messagequeuedemo.mqclient.dto.arguments.BasicArguments;

import java.io.Serializable;

/**
 * @author pluchon
 * @create 2026-03-31-10:23
 * 作者代码水平一般，难免难看，请见谅
 */
//订阅一个队列里的消息
@Getter
@Setter
public class BasicConsumeArguments extends BasicArguments implements Serializable {
    protected String consumerTag;
    private String queueName;
    private boolean autoAck;
    //consumer是一个回调的函数式接口！这里我们无法表示
    //我们服务器只需要做收到消息后，把消息返回给消费者的客户端就好了（有生产者和消费者）
    //也就是生产者客户端->消费者客户端，也就是说客户端拿到了这个消息干嘛服务器是不关心的！！
    //也就是客户端无需把自己执行的业务回调告诉服务器！因此我们不需要consumer属性成员了！！
}
