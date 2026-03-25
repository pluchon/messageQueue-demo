package org.zlh.messagequeuedemo.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
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
//我们的消息队列，和本身存在的Queue做好区分
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
    //TODO 这个也是我们后续自己实现
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String,Object> arguments = new HashMap<>();

    //从数据库的JSON字符串中取对象，并赋值到我们的arguments属性
    public void setArguments(String argumentsJson){
        try {
            this.arguments = JsonUtils.setArgumentJson(argumentsJson);
        } catch (JsonProcessingException e) {
            this.arguments = new HashMap<>();
        }
    }

    //将当前arguments以JSON格式写入数据库
    public String getArgumentsJSON(){
        try {
            return JsonUtils.getArgumentJson(arguments);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }
}
