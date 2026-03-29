package org.zlh.messagequeuedemo.mqserver;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.common.utils.router.RouterUtils;
import org.zlh.messagequeuedemo.mqserver.core.*;
import org.zlh.messagequeuedemo.mqserver.datacenter.DiskDataCenter;
import org.zlh.messagequeuedemo.mqserver.datacenter.MemoryDataCenter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author pluchon
 *         &#064;create 2026-03-29-08:47
 *         作者代码水平一般，难免难看，请见谅
 */
// 虚拟主机，每个虚拟主机管理自己的交换机，队列，绑定关系，以及消息数据，主要是保证隔离性（不同虚拟主机之间内容没有冲突）
// 对外提供API供调用，整合内存与硬盘
// 我们需要对抛出的异常进行处理
// TODO 目前我们只实现单机，后续多机以及创建/销毁机可以拓展（期望的是不同主机内部有重名的队列名等等）
@Slf4j
@Getter
public class VirtualHost {
    private String virtualHostName;
    // 引入内存与硬盘操作，需要我们主动调用初始化操作（建库建表以及示范数据）
    private DiskDataCenter diskDataCenter = new DiskDataCenter();
    // 内存中只要new出来已经被初始化了
    private MemoryDataCenter memoryDataCenter = new MemoryDataCenter();

    //锁对象，加上final保证锁一致性（不同场景下锁不同！）
    private final Object exchangeLocker = new Object();
    private final Object queueLocker = new Object();

    //虽然我们锁的粒度很大（比如A交换机操作，B交换机操作就无法执行）
    //但是我们创建/删除各个模块，属于是低密度的操作，无需频繁的获取锁/释放锁，因此出现线程冲突概率就比较低了
    //而且我们锁策略只有在我们竞争的时候才进行加锁！
    //但是注意diskDataCenter和memoryDataCenter的加锁还是有意义的！因为我们这两个类被谁调用是未知的，我们为了稳妥还是加上好！

    public VirtualHost(String virtualHostName) {
        this.virtualHostName = virtualHostName;
        diskDataCenter.init();
        // 从硬盘中恢复数据到内存中
        try {
            memoryDataCenter.recovery(diskDataCenter);
        } catch (MQException | IOException | ClassNotFoundException e) {
            log.error("[VirtualHost] 恢复内存数据失败！->{}", e.getMessage());
        }
    }

    // 核心API1->创建交换机（不存在才创建）
    // TODO 对于虚拟主机与交换机的从属关系，我们可以定义一对多的表来保存，如果不同主机有重名的直接写会无法插入，因此可以加入前缀
    // TODO 或者是给每个虚拟主机分配一个不同的数据库文件
    // 此处为了不麻烦，我们采用 交换机名字 = 虚拟主机名 + 分隔符 + 真实的交换机名字
    public boolean exchangeDeclare(String exchangeName, ExchangeTtype type, boolean isPermanent, boolean isDelete,
            Map<String, Object> argument) {
        // 把交换机名字加上虚拟主机作为前缀
        exchangeName = virtualHostName + "_" + exchangeName;
        try {
            //考虑多线程
            synchronized (exchangeLocker) {
                // 判定该交换机是否已经存在了，我们从内存中查询（硬盘只是为了持久化）
                Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                // 交换机已经存在
                if (exchange != null) {
                    // 也算创建成功
                    log.info("[VirtualHost] 交换机已经存在->" + exchangeName);
                    return true;
                }
                exchange = new Exchange();
                exchange.setName(exchangeName);
                exchange.setIsDelete(isDelete);
                exchange.setIsPermanent(isPermanent);
                exchange.setExchangeType(type);
                exchange.setArgument(argument);
                // 写入硬盘，必须是持久化为前提，写入硬盘会更容易出现异常情况
                // 如果硬盘失败了内存直接不写了（反过来还要把内存中的交换机删除，麻烦）
                if (isPermanent) {
                    diskDataCenter.insertExchange(exchange);
                }
                // 写入内存
                memoryDataCenter.insertExchange(exchange);
                log.info("[VirtualHost] 交换机创建完成->{}", exchangeName);
                return true;
            }
        } catch (Exception e) {
            log.error("[VirtualHost] 创建交换机失败->{}", exchangeName);
            return false;
        }
    }

    // 删除交换机
    public boolean exchangeDelete(String exchangeName) {
        exchangeName = virtualHostName + "_" + exchangeName;
        try {
            //多线程情况下可能另一个线程创建了这个交换机，会使得情况复杂
            synchronized (exchangeLocker) {
                // 找到对应的交换机，从内存中查询
                Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                if (exchange == null) {
                    // 找不到
                    throw new MQException("[VirtualHost] 交换机不存在->" + exchangeName);
                }
                // 找得到，我们直接删除交换机，且必须是持久化为前提才可以进行删除
                if (exchange.getIsPermanent()) {
                    diskDataCenter.deleteExchange(exchangeName);
                }
                // 从内存中删除
                memoryDataCenter.deleteExchange(exchangeName);
                log.info("[VirtualHost] 交换机删除成功->{}", exchange);
                return true;
            }
        } catch (Exception e) {
            log.error("[VirtualHost] 删除交换机失败->{}", exchangeName);
            return false;
        }
    }

    // 创建队列
    public boolean queueDeclare(String queueName, boolean isPermanet, boolean exclusive, boolean isDelete,
            Map<String, Object> argument) {
        queueName = virtualHostName + "_" + queueName;
        try {
            synchronized (queueLocker) {
                // 判断是否存在
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                // 队列存在
                if (queue != null) {
                    log.info("[VirtualHost] 队列已经存在->{}", queueName);
                    return true;
                }
                // 创建队列对象
                queue = new MSGQueue();
                queue.setExclusivel(exclusive);
                queue.setIsPermanent(isPermanet);
                queue.setIsDelete(isDelete);
                queue.setName(queueName);
                queue.setArguments(argument);
                // 写入硬盘
                if (isPermanet) {
                    diskDataCenter.insertQueue(queue);
                }
                // 写入内存
                memoryDataCenter.insertQueue(queue);
                log.info("[VirtualHost] 队列创建成功->{}", queueName);
                return true;
            }
        } catch (Exception e) {
            log.error("[VirtualHost] 创建队列失败->{}", queueName);
            return false;
        }
    }

    // 删除队列
    public boolean queueDelete(String queueName) {
        queueName = virtualHostName + "_" + queueName;
        try {
            synchronized (queueLocker) {
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                // 无法删除
                if (queue == null) {
                    throw new MQException("[VirtualHost] 队列不存在，无法删除->" + queueName);
                }
                // 可以删除，从硬盘删
                if (queue.getIsPermanent()) {
                    diskDataCenter.deleteQueue(queueName);
                }
                // 从内存中删除
                memoryDataCenter.deleteQueue(queueName);
                log.info("[VirtualHost] 队列删除成功->{}", queueName);
                return true;
            }
        } catch (Exception e) {
            log.error("[VirtualHost] 删除队列失败->{}", queueName);
            return false;
        }
    }

    // 创建绑定关系
    public boolean bingdingDeclare(String queueName, String exchangeName, String bingdingKey) {
        queueName = virtualHostName + "_" + queueName;
        exchangeName = virtualHostName + "_" + exchangeName;
        try {
            //对于绑定关系，只有同时拿到两把锁才可以进行操作，要保证和我们删除操作的加锁顺序一致性
            //这样才可以尽可能避免死锁
            synchronized (queueLocker){
                synchronized (exchangeLocker){
                    // 查询绑定关系是否已经存在
                    Bingding bingdingOnce = memoryDataCenter.getBingdingOnce(exchangeName, queueName);
                    if (bingdingOnce != null) {
                        log.error("[VirtualHost] 绑定关系已经存在->{}->{}", queueName, exchangeName);
                        return true;
                    }
                    // 验证bingdingKey是否合法
                    if (!RouterUtils.checkBingdingkey(bingdingKey)) {
                        throw new MQException("[VirtualHost] bingdingKey非法->" + bingdingKey);
                    }
                    // 创建绑定关系
                    bingdingOnce = new Bingding();
                    bingdingOnce.setQueueName(queueName);
                    bingdingOnce.setExchangeName(exchangeName);
                    bingdingOnce.setBindingKey(bingdingKey);
                    // 获取到对应的交换机与队列，如果不存在，则绑定关系也是无法创建的
                    MSGQueue queue = memoryDataCenter.getQueue(queueName);
                    if (queue == null) {
                        throw new MQException("[VirtualHost] 要绑定的队列不存在" + queueName);
                    }
                    Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                    if (exchange == null) {
                        throw new MQException("[VirtualHost] 要绑定的交换机不存在" + exchangeName);
                    }
                    // 只有都存在了才能插入绑定关系
                    // 写入硬盘，比如都要持久化
                    if (queue.getIsPermanent() && exchange.getIsPermanent()) {
                        diskDataCenter.insertBingding(bingdingOnce);
                    }
                    // 写入内存
                    memoryDataCenter.insertBingding(bingdingOnce);
                    log.info("[VirtualHost] 绑定关系创建完成->{}<-{}", queueName, exchangeName);
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("[VirtualHost] 绑定关系已经存在->{}<-{}", queueName, exchangeName);
            return false;
        }
    }

    // 销毁绑定关系
    // 注意我们绑定关系涉及到的问题->用户可能先删除队列与交换机，再来删除绑定关系，此时无法删除
    // TODO 方案一：参考类似MySQL的外键，删除时判定当前队列或交换机是否存在绑定，如果存在则禁止删除队列与交换机（解除绑定，再删除队列与交换机），麻烦！
    // 方案二：删除时候，不校验交换机与队列是否存在，直接尝试删除（✓），容易！
    public boolean bingdingDelete(String queueName, String exchangeName) {
        queueName = virtualHostName + "_" + queueName;
        exchangeName = virtualHostName + "_" + exchangeName;
        try {
            //保证和我们的呢创建队列的加锁顺序一致性
            synchronized (queueLocker){
                synchronized (exchangeLocker){
                    Bingding bingdingOnce = memoryDataCenter.getBingdingOnce(exchangeName, queueName);
                    if (bingdingOnce == null) {
                        throw new MQException("[VirtualHost] 绑定关系不存在！" + queueName + "->" + exchangeName);
                    }
                    // 查询对应的队列是否存在
                    MSGQueue queue = memoryDataCenter.getQueue(queueName);
                    boolean isQueuePermanent = (queue != null) && queue.getIsPermanent();
                    /*
                     * if(queue == null){
                     * throw new MQException("[VirtualHost] 绑定关系的队列不存在！"+queueName);
                     * }
                     */
                    Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                    boolean isExchangePermanent = (exchange != null) && exchange.getIsPermanent();
                    /*
                     * if(exchange == null){
                     * throw new MQException("[VirtualHost] 绑定关系的交换机不存在！"+exchangeName);
                     * }
                     */
                    // 持久化才能删除
                    if (isQueuePermanent && isExchangePermanent) {
                        diskDataCenter.deleteBingding(bingdingOnce);
                    }
                    // 从内存中删除
                    memoryDataCenter.deleteBingding(bingdingOnce);
                    log.info("[VirtualHost] 绑定关系删除成功！{}->{}", queueName, exchangeName);
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("[VirtualHost] 删除绑定关系失败！{}->{}", queueName, exchangeName);
            return false;
        }
    }

    //发送消息到指定的交换机->队列中
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties,byte[] body){
        try {
            //转换交换机名字
            exchangeName = virtualHostName+"_"+exchangeName;
            //检查routingKey合法性
            if(RouterUtils.checkRoutingKey(routingKey)){
                throw new MQException("[VirtualHost] routingKey非法->"+routingKey);
            }
            //查找交换机对象
            Exchange exchange = memoryDataCenter.getExchange(exchangeName);
            if(exchange == null){
                throw new MQException("[VirtualHost] 交换机不存在->"+exchangeName);
            }
            //根据其类型判断要做什么类型的转发
            ExchangeTtype type = exchange.getExchangeType();
            if(type == ExchangeTtype.DIRECT){
                //直接转发，以routingKey作为队列名，把消息写入指定队列(没有绑定没关系)
                String queueName = virtualHostName+"_"+routingKey;
                Message message = Message.messageCreateWithIDFactory(routingKey,basicProperties,body);
                //查找队列对象
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                if(queue == null){
                    throw new MQException("[VirtualHost] 队列不存在！"+queueName);
                }
                //转发消息
                sendMessage(queue,message);
                log.info("[VirtualHost] 直接交换机转发成功！{}",exchangeName);
                return true;
            }else{
                //按照fanout和topic转发
                //找到该交换机关联的所有绑定的队列
                ConcurrentHashMap<String, Bingding> stringBingdingConcurrentHashMap = memoryDataCenter.queryAllBingding(exchangeName);
                for(Map.Entry<String,Bingding> e : stringBingdingConcurrentHashMap.entrySet()){
                    //获取绑定对象，判断对嘞是否存在
                    Bingding bingding = e.getValue();
                    MSGQueue queue = memoryDataCenter.getQueue(bingding.getQueueName());
                    //说明当前绑定没有匹配的队列
                    if(queue == null){
                        //不抛出异常，可能有多处这样的队列
                        log.info("[VirtualHost] 队列不存在->"+bingding.getQueueName());
                        continue;
                    }
                    //构造消息
                    Message message = Message.messageCreateWithIDFactory(routingKey,basicProperties,body);
                    //判断这个消息能否转发给该队列！
                    //fanout->所有绑定的队列都转发，topic->校验routingKey与bingdingKey
                    //校验是否能进行转发，如果是topic要比对bingdingKey和routingKey
                    if(!RouterUtils.route(type,bingding,message)){
                        continue;
                    }
                    //转发
                    sendMessage(queue,message);
                }
                log.info("[VirtualHost] fanout&topic交换机转发成功！{}",exchangeName);
                return true;
            }
        }catch (Exception e){
            log.error("[VirtualHost] 消息发送失败->{}",exchangeName);
            return false;
        }
    }

    //发送消息，写入内存与文件中，看消息是否持久化
    private void sendMessage(MSGQueue queue, Message message) throws MQException, IOException {
        int deliverMode = message.getDeliverMode();
        //持久化
        if(deliverMode == 2){
            diskDataCenter.insertMessage(queue,message);
        }
        //写入内存
        memoryDataCenter.sendMessage(queue,message);

        //TODO 补充逻辑，通知消费者来消费消息

    }
}
