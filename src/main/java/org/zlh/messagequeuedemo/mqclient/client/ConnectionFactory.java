package org.zlh.messagequeuedemo.mqclient.client;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;

/**
 * @author pluchon
 * @create 2026-03-31-14:24
 * 作者代码水平一般，难免难看，请见谅
 */
//持有服务器地址，创建Connection连接
@Getter
@Setter
public class ConnectionFactory {
    //服务器地址
    private String host;
    //服务器端口号
    private int port;

    //TODO 以下内容自己拓展
    //虚拟主机名字
    private String virtualHostName;
    //用户名
    private String userName;
    //密码
    private String password;

    //创建新的连接
    public Connect newConnection() throws IOException {
        return new Connect(host,port);
    }
}
