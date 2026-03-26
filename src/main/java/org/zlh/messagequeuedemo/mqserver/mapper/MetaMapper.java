package org.zlh.messagequeuedemo.mqserver.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.zlh.messagequeuedemo.mqserver.core.Bingding;
import org.zlh.messagequeuedemo.mqserver.core.Exchange;
import org.zlh.messagequeuedemo.mqserver.core.MSGQueue;

import java.util.List;

/**
 * @author pluchon
 * @create 2026-03-25-23:19
 * 作者代码水平一般，难免难看，请见谅
 */
//建表
@Mapper
public interface MetaMapper {
    // 建表
    void createExchangeTable();
    void createQueueTable();
    void createBingdingTable();

    //插入与删除
    void insertExchange(Exchange exchange);
    void deleteExchange(String exchangeName);
    void insertQueue(MSGQueue msgQueue);
    void deleteQueue(String queueName);
    void insertBingding(Bingding bingding);
    //注意我们绑定删除要交换机名字和队列名字，因此传入全部参数
    void deleteBingding(Bingding bingding);

    //查找操作
    List<Exchange> queryAllExchange();
    List<MSGQueue> queryAllQueue();
    List<Bingding> queryAllBingding();
}