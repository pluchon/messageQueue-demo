package org.zlh.messagequeuedemo.common.exception;

/**
 * @author pluchon
 * @create 2026-03-27-18:44
 * 作者代码水平一般，难免难看，请见谅
 */
//自定义异常
public class MQException extends Exception {
    public MQException(String errorMsg){
        super(errorMsg);
    }
}
