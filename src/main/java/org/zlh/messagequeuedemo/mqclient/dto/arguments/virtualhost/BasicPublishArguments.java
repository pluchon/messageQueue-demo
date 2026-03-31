package org.zlh.messagequeuedemo.mqclient.dto.arguments.virtualhost;

import lombok.Getter;
import lombok.Setter;
import org.zlh.messagequeuedemo.mqclient.dto.arguments.BasicArguments;
import org.zlh.messagequeuedemo.mqserver.core.BasicProperties;

import java.io.Serializable;

/**
 * @author pluchon
 * @create 2026-03-31-10:21
 * 作者代码水平一般，难免难看，请见谅
 */
//发布消息请求参数
@Getter
@Setter
public class BasicPublishArguments extends BasicArguments implements Serializable {
    private String exchangeName;
    private BasicProperties basicProperties;
    private String routingKey;
    private byte[] content;
}
