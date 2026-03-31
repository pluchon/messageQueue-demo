package org.zlh.messagequeuedemo.common.utils.router;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.mqserver.core.Bingding;
import org.zlh.messagequeuedemo.mqserver.core.ExchangeTtype;
import org.zlh.messagequeuedemo.mqserver.core.Message;

/**
 * @author pluchon
 * @create 2026-03-29-17:17
 * 作者代码水平一般，难免难看，请见谅
 */
//侧睡我们的路由规则代码
@SpringBootTest
class RouterUtilsTest {
    // [测试用例]
    // binding key          routing key         result
    // aaa                  aaa                 true
    // aaa.bbb              aaa.bbb             true
    // aaa.bbb              aaa.bbb.ccc         false
    // aaa.bbb              aaa.ccc             false
    // aaa.bbb.ccc          aaa.bbb.ccc         true
    // aaa.*                aaa.bbb             true
    // aaa.*.bbb            aaa.bbb.ccc         false
    // *.aaa.bbb            aaa.bbb             false
    // #                    aaa.bbb.ccc         true
    // aaa.#                aaa.bbb             true
    // aaa.#                aaa.bbb.ccc         true
    // aaa.#.ccc            aaa.ccc             true
    // aaa.#.ccc            aaa.bbb.ccc         true
    // aaa.#.ccc            aaa.aaa.bbb.ccc     true
    // #.ccc                ccc                 true
    // #.ccc                aaa.bbb.ccc         true

    private RouterUtils routerUtils = new RouterUtils();
    private Bingding bingding;
    private Message message;

    @BeforeEach
    public void setUp(){
        //我们只需要测试我们的RoutingKey，不需要其他内容，因子直接new
        message = new Message();
        bingding = new Bingding();
    }

    @AfterEach
    public void tearDown(){
        message = null;
        bingding = null;
    }

    @Test
    public void testRouter1() throws MQException {
        bingding.setBindingKey("aaa");
        message.setRoutingKey("aaa");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC,bingding,message));
    }

    @Test
    public void testRouter2() throws MQException {
        bingding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter3() throws MQException {
        bingding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertFalse(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter4() throws MQException {
        bingding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.ccc");
        Assertions.assertFalse(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter5() throws MQException {
        bingding.setBindingKey("aaa.bbb.ccc");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter6() throws MQException {
        bingding.setBindingKey("aaa.*");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter7() throws MQException {
        bingding.setBindingKey("aaa.*.bbb");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertFalse(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter8() throws MQException {
        bingding.setBindingKey("*.aaa.bbb");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertFalse(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter9() throws MQException {
        bingding.setBindingKey("#");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter10() throws MQException {
        bingding.setBindingKey("aaa.#");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter11() throws MQException {
        bingding.setBindingKey("aaa.#");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter12() throws MQException {
        bingding.setBindingKey("aaa.#.ccc");
        message.setRoutingKey("aaa.ccc");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter13() throws MQException {
        bingding.setBindingKey("aaa.#.ccc");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter14() throws MQException {
        bingding.setBindingKey("aaa.#.ccc");
        message.setRoutingKey("aaa.aaa.bbb.ccc");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter15() throws MQException {
        bingding.setBindingKey("#.ccc");
        message.setRoutingKey("ccc");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter16() throws MQException {
        bingding.setBindingKey("#.ccc");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    // ================= 以下为拓展的测试用例 =================

    @Test
    public void testRouter17() throws MQException {
        // 测试：全匹配双层星号
        bingding.setBindingKey("*.*");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter18() throws MQException {
        // 测试：双层星号长度不匹配
        bingding.setBindingKey("*.*");
        message.setRoutingKey("aaa");
        Assertions.assertFalse(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter19() throws MQException {
        // 测试：# 是否能匹配0个层级
        bingding.setBindingKey("aaa.#.bbb");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter20() throws MQException {
        // 测试：包含多个 # 的情况
        bingding.setBindingKey("#.aaa.#");
        message.setRoutingKey("bbb.ccc.aaa.ddd.eee");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter21() throws MQException {
        // 测试：# 与 * 混合匹配
        bingding.setBindingKey("*.aaa.#");
        message.setRoutingKey("bbb.aaa.ccc.ddd");
        Assertions.assertTrue(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }

    @Test
    public void testRouter22() throws MQException {
        // 测试：# 与 * 混合，但由于星号必须占据一层导致不匹配
        bingding.setBindingKey("*.aaa.#");
        message.setRoutingKey("aaa.ccc.ddd");
        Assertions.assertFalse(RouterUtils.route(ExchangeTtype.TYPOIC, bingding, message));
    }
}