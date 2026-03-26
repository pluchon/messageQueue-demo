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
