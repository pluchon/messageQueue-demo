package org.zlh.messagequeuedemo.mqclient.dto.arguments.virtualhost;

import lombok.Getter;
import lombok.Setter;
import org.zlh.messagequeuedemo.mqclient.dto.arguments.BasicArguments;

import java.io.Serializable;
import java.util.Map;

/**
 * @author pluchon
 * @create 2026-03-31-10:15
 * 作者代码水平一般，难免难看，请见谅
 */
//定义队列的请求参数
@Getter
@Setter
public class QueueDeclareArguments extends BasicArguments implements Serializable {
    protected String queueName;
    private boolean isPermanet;
    private boolean exclusive;
    private boolean isDelete;
    private Map<String,Object> arguments;
}
