package org.zlh.messagequeuedemo.mqserver.core;

import lombok.AllArgsConstructor;
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
