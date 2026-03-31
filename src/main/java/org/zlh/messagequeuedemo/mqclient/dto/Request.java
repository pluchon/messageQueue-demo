package org.zlh.messagequeuedemo.mqclient.dto;

import lombok.Data;

/**
 * @author pluchon
 * @create 2026-03-31-09:28
 * 作者代码水平一般，难免难看，请见谅
 */
//按照自定义协议格式展开，请求
@Data
public class Request {
    /*
    0x1  创建 channel
    0x2  关闭 channel
    0x3  创建 exchange
    0x4  销毁 exchange
    0x5  创建 queue
    0x6  销毁 queue
    0x7  创建 binding
    0x8  销毁 binding
    0x9  发送 message
    0xa  订阅 message
    0xb  返回 ack
    0xc  服务器给客⼾端推送的消息. (被订阅的消息) 响应独有的.
     */
    private int type;
    private int length;
    private byte[] payload;
}
