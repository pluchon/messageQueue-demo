package org.zlh.messagequeuedemo.mqclient.dto.arguments.virtualhost;

import lombok.Getter;
import lombok.Setter;
import org.zlh.messagequeuedemo.mqclient.dto.arguments.BasicArguments;

import java.io.Serializable;

/**
 * @author pluchon
 * @create 2026-03-31-10:20
 * 作者代码水平一般，难免难看，请见谅
 */
//绑定关系删除
@Getter
@Setter
public class QueueBingdingDeleteArguments extends BasicArguments implements Serializable {
    private String queueName;
    private String exchangeName;
}
