package org.zlh.messagequeuedemo.mqclient.vo;

import lombok.Getter;
import lombok.Setter;
import org.zlh.messagequeuedemo.mqserver.core.BasicProperties;

import java.io.Serializable;

/**
 * @author pluchon
 * @create 2026-03-31-10:40
 * 作者代码水平一般，难免难看，请见谅
 */
//消息订阅成功的返回结果
@Getter
@Setter
public class SubScribeReturns extends BasicReturns implements Serializable {
    //消费者标识，便于客户端执行回调
    private String consumerTag;
    //参数以及消息内容
    private BasicProperties basicProperties;
    private byte[] content;
}
