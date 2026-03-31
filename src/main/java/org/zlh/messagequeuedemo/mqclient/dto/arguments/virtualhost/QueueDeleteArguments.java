package org.zlh.messagequeuedemo.mqclient.dto.arguments.virtualhost;

import lombok.Getter;
import lombok.Setter;
import org.zlh.messagequeuedemo.mqclient.dto.arguments.BasicArguments;

import java.io.Serializable;

/**
 * @author pluchon
 * @create 2026-03-31-10:18
 * 作者代码水平一般，难免难看，请见谅
 */
//队列删除的请求参数
@Getter
@Setter
public class QueueDeleteArguments extends BasicArguments implements Serializable {
    protected String queueName;
}
