package org.zlh.messagequeuedemo.mqserver.core;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * @author pluchon
 * @create 2026-03-25-15:52
 * 作者代码水平一般，难免难看，请见谅
 */
//我们定义一个交换机
@Data
public class Exchange {
    // 我们使用名字来作为每一个交换机身份的区分的唯一标识
    private String name;
    // 我们定义枚举类来区分不同类型的交换机（直接交换机，扇出交换机，主题交换机），默认是直接交换机
    private ExchangeTtype exchangeTtype = ExchangeTtype.DIRECT;
    // durable表示我们的交换机是否要进行持久化进行存储，默认给的是不进行持久化存储
    private Boolean isPermanent = false;
    // AUTODelete表示我们的队列是否要进行自动删除，如果当前交换机没有人使用了就会自动被删除了
    // TODO 我们项目后续还没有自己实现这个功能，可能可以自己尝试自己写出来？？
    private Boolean isDelete = false;
    // arguments表示我们创建交换机的时候指定的一些额外的参数的选项，虽然后续项目先不列出来，但是后续我们可以自己尝试下？？
    private Map<String,Object> argument = new HashMap<>();
}
