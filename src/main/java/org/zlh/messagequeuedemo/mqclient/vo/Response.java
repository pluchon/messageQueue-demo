package org.zlh.messagequeuedemo.mqclient.vo;

import lombok.Data;

/**
 * @author pluchon
 * @create 2026-03-31-09:29
 * 作者代码水平一般，难免难看，请见谅
 */
//响应对象，按照自定义协议
@Data
public class Response {
    private int type;
    private int length;
    private byte[] payload;
}
