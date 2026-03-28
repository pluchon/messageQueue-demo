package org.zlh.messagequeuedemo.mqserver.datacenter;

import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.mqserver.core.Bingding;
import org.zlh.messagequeuedemo.mqserver.core.Exchange;
import org.zlh.messagequeuedemo.mqserver.core.MSGQueue;
import org.zlh.messagequeuedemo.mqserver.core.Message;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author pluchon
 * @create 2026-03-28-10:33
 * 作者代码水平一般，难免难看，请见谅
 */
//使用这个类管理所有硬盘数据（数据库->交换机绑定，数据文件->消息）
//说白了就是我们进行统一的封装便于调用
public class DiskDataCenter {
    private DataBaseManager dataBaseManager = new DataBaseManager();
    private MessageFileManager messageFileManager = new MessageFileManager();

    //对上述两个实例进行初始化
    public void init(){
        dataBaseManager.init();
        messageFileManager.init();
    }

    //封装交换机操作
    public void insertExchange(Exchange exchange){
        dataBaseManager.insertExchange(exchange);
    }

    public void deleteExchange(String exchangeName){
        dataBaseManager.deleteExchange(exchangeName);
    }

    public List<Exchange> queryAllExchange(){
        return dataBaseManager.queryAllExchange();
    }

    //封装队列操作
    public void insertQueue(MSGQueue queue) throws IOException {
        //写入数据库
        dataBaseManager.insertQueue(queue);
        //创建队列对应的磁盘目录和文件
        messageFileManager.createQueue(queue.getName());
    }

    public void deleteQueue(String queueName) throws IOException {
        //删除数据库记录
        dataBaseManager.deleteQueue(queueName);
        //删除队列对应的磁盘目录和文件
        messageFileManager.deleteQueue(queueName);
    }

    public List<MSGQueue> queryAllQueue(){
        return dataBaseManager.queryAllQueue();
    }

    //封装绑定关系操作
    public void insertBingding(Bingding bingding){
        dataBaseManager.insertBingding(bingding);
    }

    public void deleteBingding(Bingding bingding){
        dataBaseManager.deleteBingding(bingding);
    }

    public List<Bingding> queryAllBingding(){
        return dataBaseManager.queryAllBingding();
    }

    //消息文件操作
    public void insertMessage(MSGQueue queue,Message message) throws MQException, IOException {
        messageFileManager.sendMessage(queue,message);
    }

    public void deleteMessage(MSGQueue queue,Message message) throws IOException, ClassNotFoundException, MQException {
        messageFileManager.deleteMessage(queue,message);
        //注意，考虑是否进行GC！！
        if(messageFileManager.checkGC(queue.getName())){
            messageFileManager.GC(queue);
        }
    }

    public LinkedList<Message> queryAllMessage(String queueName) throws MQException, IOException, ClassNotFoundException {
        return messageFileManager.queryAllMessage(queueName);
    }
}
