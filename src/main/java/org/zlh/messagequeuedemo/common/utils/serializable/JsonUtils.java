package org.zlh.messagequeuedemo.common.utils.serializable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * @author pluchon
 * @create 2026-03-26-00:00
 * 作者代码水平一般，难免难看，请见谅
 */
//JSON转换工具类
public class JsonUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    //把当前的argument内容转成JSON字符串
    public static String getArgumentJson(Object object) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(object);
    }

    // 重写方法，把数据库的JSON对象转成Map，构造我们的对象的这个属性
    public static Map<String,Object> setArgumentJson(String argumentJson) throws JsonProcessingException {
        //使用TypeReference适应复杂对象（否则直接写类名），并使用泛型参数来描述，表示我们转成的JAVA对象类型
        return OBJECT_MAPPER.readValue(argumentJson,new TypeReference<HashMap<String,Object>>(){});
    }
}
