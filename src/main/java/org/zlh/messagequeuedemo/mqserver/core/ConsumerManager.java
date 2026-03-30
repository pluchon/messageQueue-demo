package org.zlh.messagequeuedemo.mqserver.core;

import lombok.extern.slf4j.Slf4j;
import org.zlh.messagequeuedemo.common.constant.ConstantForConsumerManagerTest;
import org.zlh.messagequeuedemo.common.consumer.Consumer;
import org.zlh.messagequeuedemo.common.consumer.ConsumerEnv;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.mqserver.VirtualHost;

import java.util.concurrent.*;

/**
 * @author pluchon
 * @create 2026-03-30-10:21
 * 作者代码水平一般，难免难看，请见谅
 */
//通过这个类实现消费消息核心逻辑，也就是管理我们的消费者
@Slf4j
public class ConsumerManager {
    //持有虚拟主机实例，方便操作内存与硬盘
    private VirtualHost virtualHost;
    //指定一个线程池，执行具体的任务，消费我们的消息，固定线程数量4个
    private ExecutorService works = Executors.newFixedThreadPool(ConstantForConsumerManagerTest.FIX_POLL_SIZE);
    //存放令牌的队列（为了避免我们一个队列的消息太多导致卡顿，我们把队列的名字作为令牌，然后每一次取一条消息取消费）
    private BlockingQueue<String> blockingQueueForQueueName = new LinkedBlockingQueue<>();
    //扫描线程，看看哪个队列有新的消息了
    private Thread scnannerThread;

    //提供虚拟主机初始化
    public ConsumerManager(VirtualHost virtualHost) {
        this.virtualHost = virtualHost;
    }

    //通知进行消费，也就是把令牌放入阻塞队列中
    public void notifyConsumeMessage(String queueName) throws InterruptedException {
        blockingQueueForQueueName.put(queueName);
        scnannerThread = new Thread(()->{
            try {
                //一直扫描不停
                while (true) {
                    //获取我们的令牌
                    String queueNameForBlocking = blockingQueueForQueueName.take();
                    //获取队列
                    MSGQueue queue = virtualHost.getMemoryDataCenter().getQueue(queueName);
                    //看看是否存在
                    if (queue == null) {
                        throw new MQException("[ConsumerManager] 获取队列令牌的时候，队列不存在！" + queueName);
                    }
                    //消费消息，保证原子性！！
                    //因为其他队列也可能正在消费我们的消息
                    synchronized (queue) {
                        consumeMessage(queue);
                    }
                }
            } catch (InterruptedException | MQException e) {
                throw new RuntimeException(e);
            }
        });
        //设为后台线程（不会影响整个前台进程的结束状态）
        scnannerThread.setDaemon(true);
        scnannerThread.start();
    }

    //订阅消息，新增消费者对象到我们的队列中
    public void addConsumer(String consumerTag, String queueName, boolean autoAck, Consumer consumer) throws MQException {
        //寻找我们的对应的duilie
        MSGQueue queue = virtualHost.getMemoryDataCenter().getQueue(queueName);
        //看是否存在
        if(queue == null){
            throw new MQException("[ConsumerManager] 查找队列失败！"+queueName);
        }
        //创建我们的消费者的实例
        ConsumerEnv consumerEnv = new ConsumerEnv(consumerTag,queueName,autoAck,consumer);
        //加锁
        synchronized (queue){
            queue.addConsumerEnvList(consumerEnv);
            //注意如果我们队列中已经有了消息，则我们要进行全部消费
            int countMessage = virtualHost.getMemoryDataCenter().getQueueMessageCount(queueName);
            for(int i = 0;i < countMessage;i++){
                //消费消息
                consumeMessage(queue);
            }
        }
    }

    //消费消息
    private void consumeMessage(MSGQueue queue) {
        //轮询的方式选取一个消费者
        ConsumerEnv consumerEnv = queue.selectConsumer();
        //当前队列没有消费者，不消费，等后面再说
        if(consumerEnv == null){
            log.info("[ConsumerManager] 队列中暂时没有消费者！{}",queue.getName());
            return;
        }
        //从队列中取出一个消息
        Message message = virtualHost.getMemoryDataCenter().getMessage(queue.getName());
        //消息不存在
        if(message == null){
            log.info("[ConsumerManager] 消息不存在！{}",queue.getName());
            return;
        }
        //把消息带入到消费者的回调方法中，丢给线程池执行
        works.submit(()->{
            try {
                //不知道这个消息是不是真的被消费完毕了，可能会抛出异常，为了防止我们的消息丢失，我们可以采取一些措施
                //1. 放入ACK队列中，防止消息丢失
                //2. 执行回调
                //3. 如果当前消费者采取的是autoAck=true，认为回调执行完毕不抛出异常就算消费成功了！（删除硬盘上，删除内存上消息中心，删除ACK队列上）
                //反之，此时就属于手动应答，需要消费者这边在自己的回调方法内部显示调用basicAck
                virtualHost.getMemoryDataCenter().insertWithAckMessage(queue.getName(),message);
                //注意如果我们回调函数产生了异常，我们的后续逻辑就执行不到了，会导致这个消息始终在ack队列集合中
                //如果按照rabbitMQ的做法，可以搞一个扫描线程，判定消息在ack队列集合中存在了多久，如果超出了范围，则放入一个死信队列中
                //TODO 我们此处暂时先不实现死信队列，这个是我们创建队列的时候进行配置的
                //如果我们执行回调的时候程序崩溃了，会导致内存数据全没但硬盘数据还在，我们正在消费到消息在硬盘中存在
                //当我们程序重启之后，这个消息又被加载回内存了，会像重来没有消费到，消费者可能会有机会再次消费到这个消息
                //TODO 这个问题我们应该由消费者的业务代码考虑，我们brokerService不保证也不管了
                consumerEnv.getConsumer().handleDelivery(consumerEnv.getConsumerTag(),message.getBasicProperties(),message.getBody());
                //确认应答类型
                //手动应答暂时不去处理，让消费者手动调用basicAck处理
                if(consumerEnv.isAutoAck()){
                    //删除硬盘消息，先看看是不是持久化的
                    if(message.getDeliverMode() == 1){
                        virtualHost.getDiskDataCenter().deleteMessage(queue,message);
                    }
                    //删除内存的待确认ACK消息
                    virtualHost.getMemoryDataCenter().deleteWithAckMessage(queue.getName(),message);
                    //删除消息中心集合的消息
                    virtualHost.getMemoryDataCenter().deleteMessage(message.getMessageId());
                    log.info("[ConsumerManager] 消息被成功消费了！{}",queue.getName());
                }
            }catch (Exception e){
                log.error("[ConsumerManager] 消息消费失败！{}", queue.getName());
            }
        });
    }
}
