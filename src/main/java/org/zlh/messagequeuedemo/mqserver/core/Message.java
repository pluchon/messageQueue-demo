package org.zlh.messagequeuedemo.mqserver.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.util.StringUtils;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author pluchon
 * @create 2026-03-25-15:53
 *         作者代码水平一般，难免难看，请见谅
 */
// 我们消息队列中的一条条消息
// 主要包含两个部分（属性部分->basicProperties，正文支持二进制数据->byte）
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message implements Serializable {
    // 核心属性
    // 我们的元数据，即属性，new是避免我们的空指针
    private BasicProperties basicProperties = new BasicProperties();
    // 正文内容，可以传输图片等等;
    private byte[] body;

    // 辅助属性
    // 后续我们的Message可能会进行持久化存入文件中（性能好且我们无需频繁的进行增删改查，而且我们会一个文件中会存储很多的消息）
    // 通过以下的两个偏移量属性来找到我们某个消息在文件中具体位置，[offsetBeg,offsetEnd)采用前闭后开区间，贴合JAVA风格
    // offsetBeg表示该消息开头距离文件开头距离文件开头位置偏移量，字节
    // offsetEnd表示消息结尾距离文件开头距离文件开头位置的偏移量，字节
    // 加入transient表示我们不被序列化
    private transient long offsetBeg = 0L;
    private transient long offsetEnd = 0L;
    // isValid表示我们的消息在文件中是否是有效的消息（逻辑删除），避免我们直接删除文件内容导致后续其他消息内容的移位
    // 0x1 -> 有效，0x0 -> 无效
    // 为什么使用Byte呢，因为我们要在文件中表示，使用Byte进行统一表示比较好
    private byte isVaild = 0x1;

    // 提供快速获取消息属性的方法，这种模式被称为委派模式
    public String getMessageId() {
        return basicProperties.getMessageID();
    }

    public void setMessageId(String messageId) {
        this.basicProperties.setMessageID(messageId);
    }

    public String getRoutingKey() {
        return basicProperties.getRoutingKey();
    }

    public void setRoutingKey(String routingKey) {
        this.basicProperties.setRoutingKey(routingKey);
    }

    public void setDeliverMode(int isdeliverMode) {
        this.basicProperties.setDeliverMode(isdeliverMode);
    }

    public int getDeliverMode() {
        return basicProperties.getDeliverMode();
    }

    // 创建一个工厂方法，让工厂方法帮我们封装创建Message对象过程，最终会自动生成一个唯一的MessageId
    // 这样我们就可以避免说构造方法创建的内部细节不详造成的误会
    // 万一我们的routingkey与BasicProperties里的routingkey冲突了，我们以我们传入的routing为主
    public static Message messageCreateWithIDFactory(String routingKey, BasicProperties basicProperties, byte[] body) {
        Message messageInfo = new Message();
        // 校验逻辑
        if (basicProperties != null) {
            messageInfo.setBasicProperties(basicProperties);
        }
        if (StringUtils.hasLength(routingKey)) {
            messageInfo.setRoutingKey(routingKey);
        }
        // 使用UUID，并且加上前缀区分其他模块的UUID
        messageInfo.setMessageId("M-" + UUID.randomUUID());
        // 设置正文
        messageInfo.setBody(body);
        // 返回构造好的消息
        return messageInfo;
    }

    // 写入文件要进行序列化和反序列，我们使用标准库自带的序列化和反序列化，不使用JSON（因为JSON是文本类型的数据，和我们的二进制数据对不上）
    // 我们实现Serializable接口就好了，让我们的JAVA类能被识别，无需重写任何方法就可以使用了
    // 而且也给我们的BasicProperties不需要序列化，但是我们的offset的两个属性无需被序列化，因为我们的消息一旦被写入文件位置就固定了
}
