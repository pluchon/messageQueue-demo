package org.zlh.messagequeuedemo.mqclient.dto.arguments;

import lombok.Data;

import java.io.Serializable;

/**
 * @author pluchon
 * @create 2026-03-31-09:33
 * 作者代码水平一般，难免难看，请见谅
 */
//当前这个类表示公共的参数以及辅助字段
@Data
public class BasicArguments implements Serializable {
    //表示一次请求/响应的身份标识，把请求与响应对应上
    protected String rid;
    //这次通信使用的channel的身份标识
    protected String channelId;
}
