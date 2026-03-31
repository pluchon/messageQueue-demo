package org.zlh.messagequeuedemo.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.zlh.messagequeuedemo.mqclient.consumer.ConsumerEnv;
import org.zlh.messagequeuedemo.common.utils.serializable.JsonUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author pluchon
 * @create 2026-03-25-15:52
 *         作者代码水平一般，难免难看，请见谅
 */
// 我们的消息队列，和本身存在的Queue做好区分
@Data
public class MSGQueue {
    // 我们的消息队列的名字，是我们队列的身份的唯一标识
    private String name;
    // 队列是否要进行持久化，false表示不持久化
    private Boolean isPermanent = false;
    // exclusive表示这个队列是否只能被一个消费者使用，false表示大家共用
    // TODO 这个功能我们后续可以自己实现
    private Boolean exclusivel = false;
    // 是否在没有消费者使用的时候自动继续拿删除，false表示不会进行自动删除
    // TODO 这个后续我们还是一样自己实现
    private Boolean isDelete = false;
    // 传入我们自己的个性化参数，也是为了方便管理后续我们会JSON格式存入数据库
    // TODO 这个也是我们后续自己实现
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String, Object> arguments = new HashMap<>();

    // 扩展实现表示当前队列都有哪些消费者订阅
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private List<ConsumerEnv> consumerEnvList = new ArrayList<>();

    // 记录当前取到了第几个消费者，方便实现轮询复用
    // 使用AtomicInteger来表示原子类来操作，在多线程情况下尽可能保持一致性
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private AtomicInteger consumerIndex = new AtomicInteger(0);

    // 添加一个新的订阅者
    public void addConsumerEnvList(ConsumerEnv consumerEnv) {
        // 这里的this表示当前被调用的这个对象的实例，也就是我们的consumerEnvList
        // 外面加过一次锁了！但是没事我们是可重入锁
        synchronized (this) {
            this.consumerEnvList.add(consumerEnv);
        }
    }

    // 删除一个订阅者
    public void deleteConsumerEnvList() {

    }

    // 挑选一个订阅者，来处理当前消息（轮询挑选）
    // 只是来读取操作，并没有涉及到写操作，因此不需要加锁，但是我们为了更安全还是加上锁
    public ConsumerEnv selectConsumer() {
        synchronized (this) {
            // 该对列为空，不用通知消费者来取
            if (this.consumerEnvList.isEmpty()) {
                return null;
            }
            // 计算下标，防止越界
            int index = consumerIndex.get() % consumerEnvList.size();
            consumerIndex.getAndIncrement();
            return consumerEnvList.get(index);
        }
    }

    // 数据库中使用
    // 从数据库的JSON字符串中取对象，并赋值到我们的arguments属性
    public void setArguments(String argumentsJson) {
        try {
            this.arguments = JsonUtils.setArgumentJson(argumentsJson);
        } catch (JsonProcessingException e) {
            this.arguments = new HashMap<>();
        }
    }

    // 传入Map
    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    // 将当前 arguments 以 JSON 格式写入数据库（MyBatis #{arguments} 会调用此方法）
    public String getArguments() {
        try {
            return JsonUtils.getArgumentJson(arguments);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    // 代码中使用
    public void setArgumentsNOJSON(String key, Object value) {
        this.arguments.put(key, value);
    }

    public Map<String, Object> getArgumentsNOJSON() {
        return this.arguments;
    }
}
