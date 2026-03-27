# 硅基计划 项目开发日记 模拟实现自定义协议的消息队列  

***

[toc]

***

## 一、DAY01  

### 1. 整体项目结构预览
![image-20260326144229351](https://zlhimage.oss-cn-guangzhou.aliyuncs.com/20260326144229637.png)

### 2. mqservice-core模块  

> 注意，我们每个类方法以及属性上都有我们对应的注释，我们这里就不重复了  

#### 1. 交换机类

```java
package org.zlh.messagequeuedemo.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

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
    private ExchangeTtype exchangeType = ExchangeTtype.DIRECT;
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
    private Map<String, Object> argument = new HashMap<>();

    //注意我们不能重写我们getter和setter方法，因为他们要求的数据返回值一致

    // 数据库交互的getter与setter

    //把当前的argument内容转成JSON字符串
    public String getArgument() {
        try {
            return org.zlh.messagequeuedemo.common.utils.serializable.JsonUtils.getArgumentJson(this.argument);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    //把数据库的JSON对象转成Map，构造我们的对象的这个属性
    public void setArgument(String json) {
        try {
            this.argument = org.zlh.messagequeuedemo.common.utils.serializable.JsonUtils.setArgumentJson(json);
        } catch (JsonProcessingException e) {
            this.argument = new HashMap<>();
        }
    }

    //代码内部使用方便，例如编写测试用例

    //再提供一组argument的get与set用来更好地获取与设置键值对
    public Object getArgumentNOJSON(String key) {
        return argument.get(key);
    }

    public void setArgumentNOJSON(String key, Object value) {
        this.argument.put(key, value);
    }

    //获取到argument的MAP本身
    public Map<String, Object> getArgumentMap() {
        return this.argument;
    }
}
```

#### 2. 交换机类型的枚举类  
```java
package org.zlh.messagequeuedemo.mqserver.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/**
 * @author pluchon
 * @create 2026-03-25-15:55
 * 作者代码水平一般，难免难看，请见谅
 */
// 我们的各种交换机类型
@AllArgsConstructor
@Getter
public enum ExchangeTtype {
    DIRECT(0),
    FINOUT(1),
    TYPOIC(2),
    ;

    // 存储数字，我们后续会进行数据对应
    private final Integer type;
}
```

#### 3. 消息队列的队列主体类

```java
package org.zlh.messagequeuedemo.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.zlh.messagequeuedemo.common.utils.serializable.JsonUtils;

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
    private Map<String, Object> arguments = new HashMap<>();

    // 数据库中使用

    //从数据库的JSON字符串中取对象，并赋值到我们的arguments属性
    public void setArguments(String argumentsJson) {
        try {
            this.arguments = JsonUtils.setArgumentJson(argumentsJson);
        } catch (JsonProcessingException e) {
            this.arguments = new HashMap<>();
        }
    }

    // 将当前 arguments 以 JSON 格式写入数据库（MyBatis #{arguments} 会调用此方法）
    public String getArguments() {
        try {
            return JsonUtils.getArgumentJson(arguments);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    //代码中使用
    public void setArgumentsNOJSON(String key, Object value) {
        this.arguments.put(key, value);
    }

    public Map<String, Object> getArgumentsNOJSON() {
        return this.arguments;
    }
}
```

#### 4. 绑定关系类  

```java
package org.zlh.messagequeuedemo.mqserver.core;

import lombok.Data;

/**
 * @author pluchon
 * @create 2026-03-25-15:52
 * 作者代码水平一般，难免难看，请见谅
 */
//我们定义一个绑定类型，表示我们交换机和队列之间的交换关系
// 我们一个bingding只能表示一个和一个的绑定关系
@Data
public class Bingding {
    // 因为我们交换机和队列都是用name作为身份标识
    private String exchangeName;
    private String queueName;
    // bingkey表示我们主题交换机，是一个暗号的口令
    // 后续我们发的消息有个routingKey，要和我们的bindingKey进行校验
    private String bindingKey;

    // 为什么我们不持久化，如果我们的交换机或者是队列有一个没有持久化，那我们当前绑定持久化是没有任何意义的......
}
```

#### 5. 消息相关类  

> 消息的属性类  

```java
package org.zlh.messagequeuedemo.mqserver.core;

import lombok.Data;

import java.io.Serializable;

/**
 * @author pluchon
 * @create 2026-03-25-17:16
 * 作者代码水平一般，难免难看，请见谅
 */
//消息的属性
@Data
public class BasicProperties implements Serializable {
    // 消息的唯一的身份标识，我们使用UUID表示我们的ID唯一性
    private String messageID;
    // 如果交换机是直接交换机->我们表示我们要转发的队列名字
    // 如果是删除交换机->我们忽略
    // 如果是主题交换机，要和我们的主题交换机的BingKey一一对应
    private String routingKey;
    //deliverMode表示我们消息是否要进行持久化
    // 1->不，2->持久化
    // 不使用枚举类，因为我们的rabbitmq也是这么设计的
    private Integer deliverMode;
    // TODO 针对我们的rabbitmq来说，还是有很多别的属性的，我们可以留作我们后续进行扩展
}
```

> 消息主体类

```java
package org.zlh.messagequeuedemo.mqserver.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.util.StringUtils;

import java.beans.Transient;
import java.io.Serializable;
import java.util.UUID;

/**
 * @author pluchon
 * @create 2026-03-25-15:53
 * 作者代码水平一般，难免难看，请见谅
 */
//我们消息队列中的一条条消息
//主要包含两个部分（属性部分->basicProperties，正文支持二进制数据->byte）
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message implements Serializable {
    // 核心属性
    // 我们的元数据，即属性，new是避免我们的空指针
    private BasicProperties basicProperties = new BasicProperties();
    // 正文内容，可以传输图片等等;
    private Byte[] body;

    //辅助属性
    //后续我们的Message可能会进行持久化存入文件中（性能好且我们无需频繁的进行增删改查，而且我们会一个文件中会存储很多的消息）
    //通过以下的两个偏移量属性来找到我们某个消息在文件中具体位置，[offsetBeg,offsetEnd)采用前闭后开区间，贴合JAVA风格
    //offsetBeg表示该消息开头距离文件开头距离文件开头位置偏移量，字节
    //offsetEnd表示消息结尾距离文件开头距离文件开头位置的偏移量，字节
    //加入transient表示我们不被序列化
    private transient Long offsetBeg = 0L;
    private transient Long offsetEnd = 0L;
    // isValid表示我们的消息在文件中是否是有效的消息（逻辑删除）
    // 0x1 -> 有效，0x0 -> 无效
    // 为什么使用Byte呢，因为我们要在文件中表示，使用Byte进行统一表示比较好
    private Byte isVaild = 0x1;

    //提供快速获取消息属性的方法，这种模式被称为委派模式
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

    // 创建一个工厂方法，让工厂方法帮我们封装创建Message对象过程，最终会自动生成一个唯一的MessageId
    // 这样我们就可以避免说构造方法创建的内部细节不详造成的误会
    // 万一我们的routingkey与BasicProperties里的routingkey冲突了，我们以我们传入的routing为主
    public static Message messageCreateWithIDFactory(String routingKey,BasicProperties basicProperties,Byte[] body){
        Message messageInfo = new Message();
        // 校验逻辑
        if(basicProperties != null){
            messageInfo.setBasicProperties(basicProperties);
        }
        if(!StringUtils.hasLength(routingKey)){
            messageInfo.setRoutingKey(routingKey);
        }
        //使用UUID，并且加上前缀区分其他模块的UUID
        messageInfo.setMessageId("M-"+UUID.randomUUID());
        // 设置正文
        messageInfo.setBody(body);
        //返回构造好的消息
        return messageInfo;
    }

    //写入文件要进行序列化和反序列，我们使用标准库自带的序列化和反序列化，不使用JSON（因为JSON是文本类型的数据，和我们的二进制数据对不上）
    //我们实现Serializable接口就好了，让我们的JAVA类能被识别，无需重写任何方法就可以使用了
    //而且也给我们的BasicProperties不需要序列化，但是我们的offset的两个属性无需被序列化，因为我们的消息一旦被写入文件位置就固定了
}
```

### 3. mqservice-datacenter模块  

> 数据库管理类  

```java
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
```

### 4. mqservice-mapper模块

> 数据库层，负责增删改查

```java
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
```

### 5. 项目启动类  

```java
package org.zlh.messagequeuedemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class MessageQueueDemoApplication {
    // 特殊静态成员，指定获取某一个对象，这样我们就可以进行手动的管理
    public static ConfigurableApplicationContext context;

    public static void main(String[] args) {
        context = SpringApplication.run(MessageQueueDemoApplication.class, args);
    }

}
```

### 6. 数据库层的XML

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN"
        "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper namespace="org.zlh.messagequeuedemo.mqserver.mapper.MetaMapper">
    <!--  我们使用update进行建表操作  -->

    <!--  创建交换机表，和我们的交换机类对应  -->
    <update id="createExchangeTable">
        create table if not exists exchange(
            name varchar(50) primary key ,
            exchange_type tinyint,
            is_permanent boolean,
            is_delete boolean,
            argument varchar(1024)
        )
    </update>

    <!--  创建队列表  -->
    <update id="createQueueTable">
        create table if not exists queue(
            name varchar(50) primary key ,
            is_permanent boolean,
            exclusivel boolean,
            isDelete boolean,
            arguments varchar(1024)
        )
    </update>

    <!--  创建绑定表  -->
    <update id="createBingdingTable">
        create table if not exists bingding(
            exchange_name varchar(50),
            queue_name varchar(50),
            binding_key varchar(256),
        <!-- 使用联合主键，避免如果交换机名字重复了，我要绑定其他队列就不成功！ -->
            primary key (exchange_name, queue_name)
        )
    </update>

    <!--  插入与删除  -->
    <insert id="insertExchange" parameterType="org.zlh.messagequeuedemo.mqserver.core.Exchange">
        insert into exchange values (#{name},#{exchangeType},#{isPermanent},#{isDelete},#{argument})
    </insert>

    <delete id="deleteExchange" parameterType="java.lang.String">
        delete from exchange where name = #{exchangeName};
    </delete>

    <insert id="insertQueue" parameterType="org.zlh.messagequeuedemo.mqserver.core.MSGQueue">
        insert into queue values (#{name}, #{isPermanent}, #{exclusivel}, #{isDelete}, #{arguments});
    </insert>

    <delete id="deleteQueue" parameterType="java.lang.String">
        delete from queue where name = #{queueName};
    </delete>

    <insert id="insertBingding" parameterType="org.zlh.messagequeuedemo.mqserver.core.Bingding">
        insert into bingding values (#{exchangeName}, #{queueName}, #{bindingKey});
    </insert>

    <delete id="deleteBingding" parameterType="org.zlh.messagequeuedemo.mqserver.core.Bingding">
        delete from bingding where exchange_name = #{exchangeName} and queue_name = #{queueName};
    </delete>

    <!--  查找操作（驼峰自动转换已在 yaml 中开启，无需手写 resultMap）  -->
    <select id="queryAllExchange" resultType="org.zlh.messagequeuedemo.mqserver.core.Exchange">
        select * from exchange;
    </select>

    <select id="queryAllQueue" resultType="org.zlh.messagequeuedemo.mqserver.core.MSGQueue">
        select * from queue;
    </select>

    <select id="queryAllBingding" resultType="org.zlh.messagequeuedemo.mqserver.core.Bingding">
        select * from bingding;
    </select>

</mapper>
```

### 7. 项目配置模块  

```java
spring:
  application:
    name: messageQueue-demo
  datasource:
    # 数据文件存储的本地路径，我们SQL组织数据就是把数据存储在我们当前磁盘的一个指定的文件中
    # 当前我们指定的是什么文件作为我们的数据库文件，基准路径就是我们的当前项目所在的路径（在IDEA中）
    url: jdbc:sqlite:./data/meta.db
    # 无需用户与密码
    username:
    password:
    driver-class-name: org.sqlite.JDBC

# mybatis配置
mybatis:
  mapper-locations: classpath:mapper/**Mapper.xml
  configuration:
    map-underscore-to-camel-case: true
```

### 8. 数据库管理类测试模块

```java
package org.zlh.messagequeuedemo.mqserver.datacenter;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.zlh.messagequeuedemo.MessageQueueDemoApplication;
import org.zlh.messagequeuedemo.common.constant.ConstantForDateBaseTest;
import org.zlh.messagequeuedemo.common.constant.ConstantForTest;
import org.zlh.messagequeuedemo.mqserver.core.Bingding;
import org.zlh.messagequeuedemo.mqserver.core.Exchange;
import org.zlh.messagequeuedemo.mqserver.core.ExchangeTtype;
import org.zlh.messagequeuedemo.mqserver.core.MSGQueue;

import java.util.List;

/**
 * @author pluchon
 * @create 2026-03-26-09:23
 *         作者代码水平一般，难免难看，请见谅
 */
// 单元测试就是保证当前测试的类不收任何干扰
@SpringBootTest
@Slf4j
class DataBaseManagerTest {
    // 准备好对应类
    private final DataBaseManager dataBaseManager = new DataBaseManager();

    // 每个用例执行前都调用这个方法
    @BeforeEach
    public void setUp() {
        // 生成对象，因为我们的init中要拿到metaMapper实例对象，因此要先获取上下文内容
        MessageQueueDemoApplication.context = SpringApplication.run(MessageQueueDemoApplication.class);
        dataBaseManager.init();
    }

    // 每个用例执行后都调用这个方法
    @AfterEach
    public void tearDown() {
        // 删除数据库，注意要先关闭context上下文，因为我们context已经持有了DataBaseManager实例
        // 因此，如果DataBaseManager被别人打开使用了就无法删除（Windows），并且释放我们的8080端口
        MessageQueueDemoApplication.context.close();
        dataBaseManager.deleteDB();
    }

    // 交换机构建操作
    private Exchange createExchange(String exchangeName) {
        Exchange newExchange = new Exchange();
        newExchange.setName(exchangeName);
        newExchange.setExchangeType(ExchangeTtype.FINOUT);
        newExchange.setIsDelete(false);
        newExchange.setIsPermanent(true);
        // 使用常量替换魔法字符串
        newExchange.setArgumentNOJSON(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_KEY_1, ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_VALUE_1);
        newExchange.setArgumentNOJSON(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_KEY_2, ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_VALUE_2);
        return newExchange;
    }

    //创建队列
    private MSGQueue createQueue(String queueName) {
        MSGQueue newQueue = new MSGQueue();
        newQueue.setName(queueName);
        newQueue.setIsDelete(false);
        newQueue.setIsPermanent(true);
        newQueue.setExclusivel(false);
        newQueue.setArgumentsNOJSON(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_KEY_1, ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_VALUE_1);
        newQueue.setArgumentsNOJSON(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_KEY_2, ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_VALUE_2);
        return newQueue;
    }

    //创建绑定关系（需要先有交换机和队列才能绑定）
    private Bingding createBingding(String exchangeName, String queueName, String bindingKey) {
        Bingding bingding = new Bingding();
        bingding.setExchangeName(exchangeName);
        bingding.setQueueName(queueName);
        bingding.setBindingKey(bindingKey);
        return bingding;
    }

    // 初始化数据库方法测试
    @Test
    public void testInit() {
        List<Exchange> exchangeList = dataBaseManager.queryAllExchange();
        List<MSGQueue> msgQueueList = dataBaseManager.queryAllQueue();
        List<Bingding> bingdingList = dataBaseManager.queryAllBingding();
        //初始状态只有一个默认匿名交换机，队列和绑定都为空
        Assertions.assertEquals(1, exchangeList.size());
        Assertions.assertEquals(0, msgQueueList.size());
        Assertions.assertEquals(0, bingdingList.size());
        //校验默认交换机的具体内容
        Exchange defaultExchange = exchangeList.get(0);
        Assertions.assertEquals("", defaultExchange.getName());
        Assertions.assertEquals(ExchangeTtype.DIRECT, defaultExchange.getExchangeType());
        //默认交换机不能是 null
        Assertions.assertNotNull(defaultExchange);
        //默认交换机应该是持久化的
        Assertions.assertTrue(defaultExchange.getIsPermanent());
        //默认交换机不应该被标记为删除
        Assertions.assertFalse(defaultExchange.getIsDelete());
        log.info("初始化方法校验成功！");
    }

    // 交换机插入
    @Test
    public void testInsertExchange() {
        Exchange newExchange = createExchange(ConstantForDateBaseTest.TEST_INSERT_EXCHANGE_NAME_1);
        dataBaseManager.insertExchange(newExchange);
        List<Exchange> exchangeList = dataBaseManager.queryAllExchange();
        //插入后应有 2 条（默认 + 新插入）
        Assertions.assertEquals(2, exchangeList.size());
        Exchange newInsertExchange = exchangeList.get(1);
        // 校验各字段
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_INSERT_EXCHANGE_NAME_1, newInsertExchange.getName());
        Assertions.assertEquals(ExchangeTtype.FINOUT, newInsertExchange.getExchangeType());
        Assertions.assertFalse(newInsertExchange.getIsDelete());
        Assertions.assertTrue(newInsertExchange.getIsPermanent());
        // 校验argument参数
        Assertions.assertEquals(2, newInsertExchange.getArgumentMap().size());
        Assertions.assertEquals(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_VALUE_1,
                newInsertExchange.getArgumentNOJSON(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_KEY_1));
        Assertions.assertEquals(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_VALUE_2,
                newInsertExchange.getArgumentNOJSON(ConstantForDateBaseTest.CREATE_EXCHANGE_ARGUMENT_KEY_2));
        //查询不存在的 key 应该返回 null，不能抛异常
        Assertions.assertNull(newInsertExchange.getArgumentNOJSON("nonExistKey"));
        //对象本身不为 null
        Assertions.assertNotNull(newInsertExchange);
        log.info("交换机插入校验成功！");
    }

    // 交换机删除
    @Test
    public void testDeleteExchange() {
        Exchange newExchange = createExchange(ConstantForDateBaseTest.TEST_DELETE_EXCHANGE_NAME_1);
        dataBaseManager.insertExchange(newExchange);
        // 插入后验证存在
        List<Exchange> exchangeList = dataBaseManager.queryAllExchange();
        Assertions.assertEquals(2, exchangeList.size());
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_DELETE_EXCHANGE_NAME_1, exchangeList.get(1).getName());
        // 执行删除
        dataBaseManager.deleteExchange(ConstantForDateBaseTest.TEST_DELETE_EXCHANGE_NAME_1);
        // 删除后验证：只剩默认交换机
        exchangeList = dataBaseManager.queryAllExchange();
        Assertions.assertEquals(1, exchangeList.size());
        Assertions.assertEquals("", exchangeList.get(0).getName());
        //删除一个不存在的交换机名，不应该抛异常
        Assertions.assertDoesNotThrow(() -> dataBaseManager.deleteExchange("nonExistExchange"));
        //删除不存在的交换机后，数量仍然不变（还是 1）
        exchangeList = dataBaseManager.queryAllExchange();
        Assertions.assertEquals(1, exchangeList.size());
        log.info("交换机删除校验成功！");
    }

    // 队列插入
    @Test
    public void testInsertQueue() {
        MSGQueue newInsertQueue = createQueue(ConstantForDateBaseTest.TEST_INSERT_QUEUE_NAME_1);
        dataBaseManager.insertQueue(newInsertQueue);
        List<MSGQueue> msgQueueList = dataBaseManager.queryAllQueue();
        //注意我们最开始没有任何队列数据，我们插入后只有一条队列数据，这个和之前的Exchange交换机不一样
        Assertions.assertEquals(1, msgQueueList.size());
        MSGQueue newQueue = msgQueueList.get(0);
        //校验各字段
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_INSERT_QUEUE_NAME_1, newQueue.getName());
        Assertions.assertTrue(newQueue.getIsPermanent());
        Assertions.assertFalse(newQueue.getIsDelete());
        Assertions.assertFalse(newQueue.getExclusivel());
        //校验 argument 的两个 MAP 参数
        Assertions.assertEquals(2, newQueue.getArgumentsNOJSON().size());
        Assertions.assertEquals(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_VALUE_1,
                newQueue.getArgumentsNOJSON().get(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_KEY_1));
        Assertions.assertEquals(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_VALUE_2,
                newQueue.getArgumentsNOJSON().get(ConstantForDateBaseTest.CREATE_QUEUE_ARGUMENT_KEY_2));
        //对象本身不为 null
        Assertions.assertNotNull(newQueue);
        //查询不存在的 key 返回 null
        Assertions.assertNull(newQueue.getArgumentsNOJSON().get("nonExistKey"));
        log.info("队列插入校验成功！");
    }

    //队列删除
    @Test
    public void testDeleteQueue() {
        MSGQueue newQueue = createQueue(ConstantForDateBaseTest.TEST_DELETE_QUEUE_NAME_1);
        dataBaseManager.insertQueue(newQueue);
        //插入后验证存在
        List<MSGQueue> msgQueueList = dataBaseManager.queryAllQueue();
        Assertions.assertEquals(1, msgQueueList.size());
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_DELETE_QUEUE_NAME_1, msgQueueList.get(0).getName());
        //执行删除
        dataBaseManager.deleteQueue(ConstantForDateBaseTest.TEST_DELETE_QUEUE_NAME_1);
        //删除后验证：列表应为空
        msgQueueList = dataBaseManager.queryAllQueue();
        Assertions.assertEquals(0, msgQueueList.size());
        //删除一个不存在的队列名，不应该抛异常
        Assertions.assertDoesNotThrow(() -> dataBaseManager.deleteQueue("nonExistQueue"));
        //删除不存在的队列后数量仍然是 0
        msgQueueList = dataBaseManager.queryAllQueue();
        Assertions.assertEquals(0, msgQueueList.size());
        log.info("队列删除校验成功！");
    }

    //绑定插入
    @Test
    public void testInsertBingding() {
        //绑定需要依赖已存在的交换机与队列，先创建它们
        Exchange exchange = createExchange(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME);
        MSGQueue queue1 = createQueue(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_1);
        MSGQueue queue2 = createQueue(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_2);
        dataBaseManager.insertExchange(exchange);
        dataBaseManager.insertQueue(queue1);
        dataBaseManager.insertQueue(queue2);
        //创建两条绑定关系：同一个交换机绑定两个不同队列
        Bingding bingding1 = createBingding(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME,
                ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_1, ConstantForDateBaseTest.TEST_BINGDING_BINDING_KEY);
        Bingding bingding2 = createBingding(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME,
                ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_2, ConstantForDateBaseTest.TEST_BINGDING_BINDING_KEY);
        dataBaseManager.insertBingding(bingding1);
        dataBaseManager.insertBingding(bingding2);
        //查询验证
        List<Bingding> bingdingList = dataBaseManager.queryAllBingding();
        Assertions.assertEquals(2, bingdingList.size());
        //校验第一条绑定的具体字段
        Bingding result1 = bingdingList.get(0);
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME, result1.getExchangeName());
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_1, result1.getQueueName());
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_BINGDING_BINDING_KEY, result1.getBindingKey());
        //对象本身不为 null
        Assertions.assertNotNull(result1);
        log.info("绑定插入校验成功！");
    }

    //绑定删除
    @Test
    public void testDeleteBingding() {
        //同上，先建好交换机和队列
        Exchange exchange = createExchange(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME);
        MSGQueue queue1 = createQueue(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_1);
        MSGQueue queue2 = createQueue(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_2);
        dataBaseManager.insertExchange(exchange);
        dataBaseManager.insertQueue(queue1);
        dataBaseManager.insertQueue(queue2);
        //插入两条绑定关系
        Bingding bingding1 = createBingding(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME,
                ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_1, ConstantForDateBaseTest.TEST_BINGDING_BINDING_KEY);
        Bingding bingding2 = createBingding(ConstantForDateBaseTest.TEST_BINGDING_EXCHANGE_NAME,
                ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_2, ConstantForDateBaseTest.TEST_BINGDING_BINDING_KEY);
        dataBaseManager.insertBingding(bingding1);
        dataBaseManager.insertBingding(bingding2);
        //删除第一条绑定
        dataBaseManager.deleteBingding(bingding1);
        //验证只剩一条，且是 queue2 的那条
        List<Bingding> bingdingList = dataBaseManager.queryAllBingding();
        Assertions.assertEquals(1, bingdingList.size());
        Assertions.assertEquals(ConstantForDateBaseTest.TEST_BINGDING_QUEUE_NAME_2, bingdingList.get(0).getQueueName());
        //再删除第二条
        dataBaseManager.deleteBingding(bingding2);
        bingdingList = dataBaseManager.queryAllBingding();
        Assertions.assertEquals(0, bingdingList.size());
        //删除不存在的绑定不应该抛异常
        Bingding nonExist = createBingding("nonExist", "nonExist", "nonExist");
        Assertions.assertDoesNotThrow(() -> dataBaseManager.deleteBingding(nonExist));
        //删除后数量仍是 0
        Assertions.assertEquals(0, dataBaseManager.queryAllBingding().size());
        log.info("绑定删除校验成功！");
    }
}
```

## 二、DAY02  
