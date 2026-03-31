package org.zlh.messagequeuedemo.mqclient.vo;

import lombok.Data;

import java.io.Serializable;

/**
 * @author pluchon
 * @create 2026-03-31-09:36
 * 作者代码水平一般，难免难看，请见谅
 */
//当前这个类表示我们各个远程调用的方法的返回值的公共信息
@Data
public class BasicReturns implements Serializable {
    //表示一次请求/响应的身份标识，把请求与响应对应上
    protected String rid;
    //这次通信使用的channel的身份标识
    protected String channelId;
    //方法boolean返回值
    protected boolean isOk;
}
