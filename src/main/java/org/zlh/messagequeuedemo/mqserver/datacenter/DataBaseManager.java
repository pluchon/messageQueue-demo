package org.zlh.messagequeuedemo.mqserver.datacenter;

import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zlh.messagequeuedemo.MessageQueueDemoApplication;
import org.zlh.messagequeuedemo.mqserver.core.Bingding;
import org.zlh.messagequeuedemo.mqserver.core.Exchange;
import org.zlh.messagequeuedemo.mqserver.core.ExchangeTtype;
import org.zlh.messagequeuedemo.mqserver.core.MSGQueue;
import org.zlh.messagequeuedemo.mqserver.mapper.MetaMapper;

import java.io.File;
import java.util.List;

/**
 * @author pluchon
 * @create 2026-03-26-00:35
 *         作者代码水平一般，难免难看，请见谅
 */
// 通过这个类取进行我们数据库的操作
// 我们要注入metaMapper字段，但是又不想让spring管理DataBaseManager这个类
@Slf4j
public class DataBaseManager {
    private MetaMapper metaMapper;

    // 对数据库进行初始化->建库建表操作并插入一些默认的数据
    // 如果已经存在库和表，啥都不干，否则我们就建库建表并插入一些默认的数据
    public void init() {
        // 手动获取我们的metaMapper对象，避免后续的空指针异常
        metaMapper = MessageQueueDemoApplication.context.getBean(MetaMapper.class);
        if (!checkDBExists()) {
            //创建目录
            File dataDir = new File("./data");
            boolean buildDir = dataDir.mkdirs();
            if(!buildDir){
                log.info("[DataBaseManager] 创建失败，目录可能已存在！");
            }
            // 执行建库建表操作
            createTable();
            createDefaultData();
            log.info("[DataBaseManager] 数据库创建完成！");
        } else {
            log.info("[DataBaseManager] 数据库已经存在！");
        }
    }

    //删除数据库文件
    public void deleteDB(){
        File dbFile = new File("./data/meta.db");
        if (dbFile.delete()) {
            log.info("[DataBaseManager] 删除数据库文件成功！");
        } else {
            log.info("[DataBaseManager] 删除数据库文件失败！");
        }
        //删除目录
        File dbDir = new File("./data");
        if(dbDir.delete()){
            log.info("[DataBaseManager] 删除数据库目录成功！");
        }else{
            log.info("[DataBaseManager] 删除数据库目录失败，可能已经被删除！");
        }
    }

    // 对外方法
    public void insertExchange(Exchange exchange) {
        metaMapper.insertExchange(exchange);
        log.info("[DataBaseManager] 交换机插入成功，name={}", exchange.getName());
    }

    public void deleteExchange(String exchangeName) {
        metaMapper.deleteExchange(exchangeName);
        log.info("[DataBaseManager] 交换机删除成功，name={}", exchangeName);
    }

    public void insertQueue(MSGQueue msgQueue) {
        metaMapper.insertQueue(msgQueue);
        log.info("[DataBaseManager] 队列插入成功，name={}", msgQueue.getName());
    }

    public void deleteQueue(String queueName) {
        metaMapper.deleteQueue(queueName);
        log.info("[DataBaseManager] 队列删除成功，name={}", queueName);
    }

    public void insertBingding(Bingding bingding) {
        metaMapper.insertBingding(bingding);
        log.info("[DataBaseManager] 绑定关系插入成功，exchangeName={}，queueName={}",
                bingding.getExchangeName(), bingding.getQueueName());
    }

    public void deleteBingding(Bingding bingding) {
        metaMapper.deleteBingding(bingding);
        log.info("[DataBaseManager] 绑定关系删除成功，exchangeName={}，queueName={}",
                bingding.getExchangeName(), bingding.getQueueName());
    }

    public List<Exchange> queryAllExchange() {
        List<Exchange> exchanges = metaMapper.queryAllExchange();
        log.info("[DataBaseManager] 查询所有交换机，共 {} 条", exchanges.size());
        return exchanges;
    }

    public List<MSGQueue> queryAllQueue() {
        List<MSGQueue> queues = metaMapper.queryAllQueue();
        log.info("[DataBaseManager] 查询所有队列，共 {} 条", queues.size());
        return queues;
    }

    public List<Bingding> queryAllBingding() {
        List<Bingding> bingdings = metaMapper.queryAllBingding();
        log.info("[DataBaseManager] 查询所有绑定关系，共 {} 条", bingdings.size());
        return bingdings;
    }

    // ----------------------------------------------------------

    // 判定数据库文件是否存在
    private boolean checkDBExists() {
        File dbFile = new File("./data/meta.db");
        return dbFile.exists();
    }

    // 创建表，建库操作我们无需手动执行
    // mybatis自动帮我们完成了创建库meta.db这个数据库文件
    private void createTable() {
        metaMapper.createBingdingTable();
        metaMapper.createQueueTable();
        metaMapper.createExchangeTable();
        log.info("[DataBaseManager] 表创建成功！");
    }

    // 创建数据库的默认数据，主要是添加默认交换机（对应我们RabbitMQ带有一个匿名的直接交换机）
    private void createDefaultData() {
        Exchange defaultExchange = new Exchange();
        defaultExchange.setName("");
        defaultExchange.setExchangeType(ExchangeTtype.DIRECT);
        defaultExchange.setIsPermanent(true);
        defaultExchange.setIsDelete(false);
        metaMapper.insertExchange(defaultExchange);
        log.info("[DataBaseManager] 默认交换机创建完成");
    }
}
