package org.zlh.messagequeuedemo.mqclient.dto.arguments.virtualhost;

import lombok.Getter;
import lombok.Setter;
import org.zlh.messagequeuedemo.mqclient.dto.arguments.BasicArguments;
import org.zlh.messagequeuedemo.mqserver.core.ExchangeTtype;

import java.io.Serializable;
import java.util.Map;

/**
 * @author pluchon
 * @create 2026-03-31-09:43
 * 作者代码水平一般，难免难看，请见谅
 */
//交换机定义的参数的请求
@Getter
@Setter
public class ExchangeDeclareArguments extends BasicArguments implements Serializable {
    //当前这个方法独有参数
    private String exchangeName;
    private ExchangeTtype exchangeTtype;
    private boolean isPermanet;
    private boolean isDelete;
    private Map<String,Object> arguments;
}
