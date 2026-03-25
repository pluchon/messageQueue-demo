package org.zlh.messagequeuedemo.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.zlh.messagequeuedemo.common.utils.JsonUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author pluchon
 * @create 2026-03-25-15:52
 * 作者代码水平一般，难免难看，请见谅
 */
//我们定义一个交换机
@Data
public class Exchange {
    // 我们使用名字来作为每一个交换机身份的区分的唯一标识
    private String name;
    // 我们定义枚举类来区分不同类型的交换机（直接交换机，扇出交换机，主题交换机），默认是直接交换机
    private ExchangeTtype exchangeTtype = ExchangeTtype.DIRECT;
    // durable表示我们的交换机是否要进行持久化进行存储，默认给的是不进行持久化存储
    private Boolean isPermanent = false;
    // AUTODelete表示我们的队列是否要进行自动删除，如果当前交换机没有生产者使用了就会自动被删除了
    // TODO 我们项目后续还没有自己实现这个功能，可能可以自己尝试自己写出来？？
    private Boolean isDelete = false;
    // arguments表示我们创建交换机的时候指定的一些额外的参数的选项，虽然后续项目先不列出来，但是后续我们可以自己尝试下？？
    // 为了存入数据库，因此我们可以转换成JSON格式字符串存入数据库
    // 告诉DATA注解这个参数无需自动生成get与set，让我们来自己写
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String,Object> argument = new HashMap<>();

    //注意我们不能重写我们getter和setter方法，因为他们要求的数据返回值一致

    //把当前的argument内容转成JSON字符串
    public String getArguments() {
        try {
            return JsonUtils.getArgumentJson(this.argument);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    //把数据库的JSON对象转成Map，构造我们的对象的这个属性
    public void setArguments(String json) {
        try {
            this.argument = JsonUtils.setArgumentJson(json);
        } catch (JsonProcessingException e) {
            this.argument = new HashMap<>();
        }
    }
}
