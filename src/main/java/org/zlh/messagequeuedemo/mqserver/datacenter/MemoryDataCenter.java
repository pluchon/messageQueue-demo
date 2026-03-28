package org.zlh.messagequeuedemo.mqserver.datacenter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.mqserver.core.Bingding;
import org.zlh.messagequeuedemo.mqserver.core.Exchange;
import org.zlh.messagequeuedemo.mqserver.core.MSGQueue;
import org.zlh.messagequeuedemo.mqserver.core.Message;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author pluchon
 * @create 2026-03-28-12:40
 * 作者代码水平一般，难免难看，请见谅
 */
//管理内存中的数据，而且我们要线程安全
@Slf4j
public class MemoryDataCenter {
    //String表示我们交换机名字，Value就是我们的值
    private ConcurrentHashMap<String, Exchange> exchangeConcurrentHashMap = new ConcurrentHashMap<>();
    //String表示我们队列名字，Value就是我们的值
    private ConcurrentHashMap<String, MSGQueue> msgQueueConcurrentHashMap = new ConcurrentHashMap<>();
    //key1->交换机名字，Key2->队列名字，Value->就是我们的值
    private ConcurrentHashMap<String,ConcurrentHashMap<String, Bingding>> bingdingsConcurrentHashMap = new ConcurrentHashMap<>();
    //String->消息ID，Value->值
    private ConcurrentHashMap<String, Message> messageConcurrentHashMap = new ConcurrentHashMap<>();
    //String->队列名字，Value->这个消息队列中所有的消息
    private ConcurrentHashMap<String, LinkedList<Message>> queueMessageConcurrentHashMap = new ConcurrentHashMap<>();
    //Key1->队列名字，Key2->消息ID，Value->消息本体，后续我们会实现消息确认逻辑使用HashMap方便进行查找
    private ConcurrentHashMap<String,ConcurrentHashMap<String,Message>> ackMessageQueueConcurrentHashMap = new ConcurrentHashMap<>();

    //交换机
    public void insertExchange(Exchange exchange){
        exchangeConcurrentHashMap.put(exchange.getName(),exchange);
        log.info("[MemoryDataCenter] 交换机添加成功，exchangeName={}", exchange.getName());
    }

    public Exchange getExchange(String exchangeName){
        return exchangeConcurrentHashMap.get(exchangeName);
    }

    public void deleteExchange(String exchangeName){
        exchangeConcurrentHashMap.remove(exchangeName);
        log.info("[MemoryDataCenter] 交换机移除成功，exchangeName={}", exchangeName);
    }

    //队列
    public void insertQueue(MSGQueue queue){
        msgQueueConcurrentHashMap.put(queue.getName(),queue);
        log.info("[MemoryDataCenter] 队列添加成功，queueName={}", queue.getName());
    }

    public MSGQueue getQueue(String queueName){
        return msgQueueConcurrentHashMap.get(queueName);
    }

    public void deleteQueue(String queueName){
        msgQueueConcurrentHashMap.remove(queueName);
        log.info("[MemoryDataCenter] 队列移除成功，queueName={}", queueName);
    }

    //绑定关系，注意嵌套哈希表！
    public void insertBingding(Bingding bingding) throws MQException {
        //获取交换机名字以及队列名字，查看是否已经存在
        String exchangeName = bingding.getExchangeName();
        String queueName = bingding.getQueueName();
        //为空才插入，这个computeIfAbsenthi校验你的Key存不存在，不存在则执行后面的代码逻辑（lambda表达式）
        ConcurrentHashMap<String, Bingding> bingdingMap = bingdingsConcurrentHashMap
                .computeIfAbsent(exchangeName, k -> new ConcurrentHashMap<>());
        //针对数据进一步查询，而且以下代码要线程安全（因为我们get和put是两步的操作，是有前因后果的）
        //我们针对bingdingMap操作，就对它进行加锁操作
        synchronized(bingdingMap){
            Bingding getBingding = bingdingMap.get(queueName);
            //说明这个绑定关系已经存在了，不能再次切换绑定
            if(getBingding != null){
                throw new MQException("[MemoryDataCenter] 绑定关系已经存在"+exchangeName+"->"+queueName);
            }
            //正式进行插入
            bingdingMap.put(queueName,bingding);
            log.info("[MemoryDataCenter] 绑定关系添加成功，exchangeName={} -> queueName={}", exchangeName, queueName);
        }
    }

    //获取指定的队列与交换机的绑定
    public Bingding getBingdingOnce(String exchangeName,String queueName){
        ConcurrentHashMap<String, Bingding> stringBingdingConcurrentHashMap = bingdingsConcurrentHashMap.get(exchangeName);
        //该交换机没有绑定到任何队列
        if(stringBingdingConcurrentHashMap == null){
            log.info("[MemoryDataCenter] 该交换机没有绑定任何队列: exchangeName={}", exchangeName);
            return null;
        }
        return stringBingdingConcurrentHashMap.get(queueName);
    }

    //获取该交换机的所有绑定关系
    public ConcurrentHashMap<String,Bingding> queryAllBingding(String exchangeName){
        return bingdingsConcurrentHashMap.get(exchangeName);
    }

    //删除绑定关系
    public void deleteBingding(Bingding bingding) throws MQException {
        String queueName = bingding.getQueueName();
        String exchangeName = bingding.getExchangeName();
        ConcurrentHashMap<String,Bingding> stringBingdingConcurrentHashMap = bingdingsConcurrentHashMap.get(exchangeName);
        //该交换机没有绑定任何队列
        if(stringBingdingConcurrentHashMap == null){
            throw new MQException("[MemoryDataCenter] 绑定关系不存在！"+queueName+"->"+exchangeName);
        }
        //可以删除
        stringBingdingConcurrentHashMap.remove(queueName);
        log.info("[MemoryDataCenter] 绑定关系移除成功，exchangeName={} -> queueName={}", exchangeName, queueName);
    }

    //插入消息到消息总表中
    public void insertMessage(Message message){
        messageConcurrentHashMap.put(message.getMessageId(),message);
        log.info("[MemoryDataCenter] 新消息添加完成，messageId={}", message.getMessageId());
    }

    //根据消息ID查询消息
    public Message getMessageWithId(String messageId){
        return messageConcurrentHashMap.get(messageId);
    }

    //删除消息
    public void deleteMessage(String messageId){
        messageConcurrentHashMap.remove(messageId);
        log.info("[MemoryDataCenter] 消息移除成功，messageId={}", messageId);
    }

    //发送消息到指定的队列，也就是把消息放入queueMessageConcurrentHashMap中
    //并且要考虑多线程
    public void sendMessage(MSGQueue queue,Message message){
        String queueName = queue.getName();
        //说明该队列还没有任何消息，因此们创建这个链表，进行判断（之前用过了）
        //这里computeIfAbsent是线程安全的
        LinkedList<Message> messageLinkedList = queueMessageConcurrentHashMap.computeIfAbsent(queueName, k -> new LinkedList<>());
        //这里线程不安全
        synchronized (messageLinkedList) {
            //放入我们的消息
            messageLinkedList.add(message);
        }
        //插入到我们的总的消息表中，重复插入也没关系，我们重点是MessageId与Message内容要对应就好
        insertMessage(message);
        log.info("[MemoryDataCenter] 消息发送成功，messageId={}->queueName={}",message.getMessageId(),queue.getName());
    }

    //从队列中获取消息，取一条
    public Message getMessage(String queueName){
        //查找，如果不存在则说明没有消息（队列从未通过 sendMessage 写入过消息）
        LinkedList<Message> messageLinkedList = queueMessageConcurrentHashMap.get(queueName);
        if (messageLinkedList == null) {
            return null;
        }
        //注意如果是空则我们不能进行加锁
        //注意：isEmpty 也必须放在锁内！否则判断完 isEmpty==false 后，另一个线程可能抢先取走了消息，
        //再执行 remove(0) 时链表已经空了，导致 IndexOutOfBoundsException
        synchronized (messageLinkedList) {
            if (messageLinkedList.isEmpty()) {
                return null;
            }
            //取头部元素，进行头删
            Message message = messageLinkedList.remove(0);
            log.info("[MemoryDataCenter] 消息从队列中取出，queueName={}，messageId={}", queueName, message.getMessageId());
            return message;
        }
    }

    //获取我们指定队列的消息的个数
    public int getQueueMessageCount(String queueName){
        LinkedList<Message> messageLinkedList = queueMessageConcurrentHashMap.get(queueName);
        if(messageLinkedList == null){
            return 0;
        }
        synchronized (messageLinkedList) {
            return messageLinkedList.size();
        }
    }

    //未确认消息添加
    //Key1->队列名字，Key2->MessageId，Value->消息
    public void insertWithAckMessage(String queueName,Message message){
        ConcurrentHashMap<String, Message> stringMessageConcurrentHashMap = ackMessageQueueConcurrentHashMap
                .computeIfAbsent(queueName,k -> new ConcurrentHashMap<>());
        stringMessageConcurrentHashMap.put(message.getMessageId(),message);
        log.info("[MemoryDataCenter] 未确认消息添加成功，queueName={}，messageId={}", queueName, message.getMessageId());
    }

    //删除未确认消息
    public void deleteWithAckMessage(String queueName,Message message){
        ConcurrentHashMap<String, Message> stringMessageConcurrentHashMap = ackMessageQueueConcurrentHashMap.get(queueName);
        //没有这个待确认消息，无需删除
        if(stringMessageConcurrentHashMap == null){
            return;
        }
        stringMessageConcurrentHashMap.remove(message.getMessageId());
        log.info("[MemoryDataCenter] 未确认消息删除成功，queueName={}，messageId={}", queueName, message.getMessageId());
    }

    //获取指定的未确认消息
    public Message getWithAckMessage(String queueName,String messageId){
        ConcurrentHashMap<String, Message> stringMessageConcurrentHashMap = ackMessageQueueConcurrentHashMap.get(queueName);
        if(stringMessageConcurrentHashMap == null){
            return null;
        }
        return stringMessageConcurrentHashMap.get(messageId);
    }

    //从硬盘上读取数据，放入内存中
    public void recovery(DiskDataCenter diskDataCenter) throws MQException, IOException, ClassNotFoundException {
        //先清楚所有的交换机内的数据
        exchangeConcurrentHashMap.clear();
        //先清空所以的队列数据
        msgQueueConcurrentHashMap.clear();
        //先清空所有的绑定关系数据
        bingdingsConcurrentHashMap.clear();
        //先清空所有的消息数据
        messageConcurrentHashMap.clear();
        //恢复所有交换机数据
        List<Exchange> exchangeList = diskDataCenter.queryAllExchange();
        for(Exchange exchangeInfo : exchangeList){
            exchangeConcurrentHashMap.put(exchangeInfo.getName(),exchangeInfo);
        }
        //恢复所有的队列数据
        List<MSGQueue> msgQueueList = diskDataCenter.queryAllQueue();
        for(MSGQueue queueInfo : msgQueueList){
            msgQueueConcurrentHashMap.put(queueInfo.getName(),queueInfo);
        }
        //恢复所有的绑定关系数据
        List<Bingding> bingdings = diskDataCenter.queryAllBingding();
        for(Bingding bingdingInfo : bingdings){
            String exchageName = bingdingInfo.getExchangeName();
            ConcurrentHashMap<String, Bingding> stringBingdingConcurrentHashMap = bingdingsConcurrentHashMap
                    .computeIfAbsent(exchageName,k -> new ConcurrentHashMap<>());
            stringBingdingConcurrentHashMap.put(bingdingInfo.getQueueName(),bingdingInfo);
        }
        //恢复所有的消息数据，先遍历所有队列获取其名字，再获得其所有的消息
        for(MSGQueue queueInfo : msgQueueList){
            String queueName = queueInfo.getName();
            LinkedList<Message> messageLinkedList = diskDataCenter.queryAllMessage(queueName);
            queueMessageConcurrentHashMap.put(queueName,messageLinkedList);
            //把每一个消息添加到消息中心
            for(Message messageInfo : messageLinkedList){
                messageConcurrentHashMap.put(messageInfo.getMessageId(),messageInfo);
            }
        }
        
        log.info("[MemoryDataCenter] 从硬盘恢复数据完成！恢复了交换机 {} 个，队列 {} 个，绑定关系 {} 个，消息 {} 条",
                exchangeConcurrentHashMap.size(),
                msgQueueConcurrentHashMap.size(),
                bingdings.size(),
                messageConcurrentHashMap.size());

        //为什么不用管ACK？因为我们取了消息但是没有应答，就算是没有真正消息到
        //一旦等待ACK过程中服务器重启了，此时我们这些"未被确认消息"->"未被取走的消息"，让消费者重新来获取
    }
}
