package org.zlh.messagequeuedemo.common.utils.router;

import lombok.Getter;
import org.springframework.util.StringUtils;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.mqserver.core.Bingding;
import org.zlh.messagequeuedemo.mqserver.core.ExchangeTtype;
import org.zlh.messagequeuedemo.mqserver.core.Message;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * @author pluchon
 * @create 2026-03-29-09:51
 * 作者代码水平一般，难免难看，请见谅
 */
//实现交换机转发规则，借助这个类验证bingdingKey的合法性，这个类就是描述的是路由的规则
public class RouterUtils {
    //*->匹配任何一个独立部分，#->可以匹配任何0/多个独立的部分

    //验证bingdingKey的合法性，它是一把锁，携带在交换机中
    //bingdingKeu组成：1. 数字，字母，下划线组成
    //2. 我们把routingKey分割多个部分内容，类似于"aaa.bbb.110"
    //3. 持有两种特殊符号作为通配符（*,#），且是一个独立的字段（被.分割），类似于"aaa.*.bbb"
    public static boolean checkBingdingkey(String bingdingKey){
        //空串检查，在直接交换机/删除交换机的时候为空
        if(!StringUtils.hasLength(bingdingKey)){
            return true;
        }
        //逐一校验
        for(char ch : bingdingKey.toCharArray()){
            //检查是不是大写字母，是不是小写字母
            if(Character.isUpperCase(ch) || Character.isLowerCase(ch)){
                continue;
            }
            //检查是否是阿拉伯数字
            if(ch >= '0' && ch <= '9'){
                continue;
            }
            //判定是否是下划线和点，检查是否是通配符，且只能占据一个位置
            if(ch == '_' || ch == '.' || ch == '*' || ch == '#'){
                continue;
            }
            return false;
        }
        //检查*和#是否是独立的部分：aaa.*.bbb ✓ aaa.a*.bbb ✕
        //按照.切割，要转换为正则表达式（"."在正则表达式中是一个特殊符号，但是我们又想让其为一个原始的文本对待，要转义）
        //而且要在Java中"\."又是特殊字符，因此要再次转义！
        String[] bingdingKeySplit = bingdingKey.split("\\.");
        //遍历分段
        for(String str : bingdingKeySplit){
            //如果长度>1且包含*和#，则我们定性为非法
            if(str.length() > 1 && (str.contains("*") || str.contains("#"))){
                return false;
            }
        }
        //约定通配符之间的相邻关系
        //1. aaa.#.#.bbb ✕
        //2. aaa.#.*.bbb ✕
        //3. aaa.*.#.bbb ✕
        //TODO 以上三种实现起来太复杂了，而且功能性上提升不大
        //4. aaa.*.*.bbb ✓
        for(int i = 0;i < bingdingKeySplit.length-1;i++){
            //判断是否是连续两个#
            if((bingdingKeySplit[i].equals("#") && bingdingKeySplit[i+1].equals("#")) ||
                    (bingdingKeySplit[i].equals("#") && bingdingKeySplit[i+1].equals("*")) ||
                    (bingdingKeySplit[i].equals("*") && bingdingKeySplit[i+1].equals("#"))){
                return false;
            }
        }
        return true;
    }

    //验证routingKey的合法性，它是一把钥匙，携带在消息属性中
    //routingKeu组成：1. 数字，字母，下划线组成
    //2. 我们把routingKey分割多个部分内容，类似于"aaa.bbb.110"
    public static boolean checkRoutingKey(String routingKey){
        //空串，在使用fanout交换机时候是合法的，设为""
        if(!StringUtils.hasLength(routingKey)){
            return true;
        }
        //循环逐个校验字符
        for(char ch : routingKey.toCharArray()){
            //检查是不是大写字母，是不是小写字母
            if(Character.isUpperCase(ch) || Character.isLowerCase(ch)){
                continue;
            }
            //检查是否是阿拉伯数字
            if(ch >= '0' && ch <= '9'){
                continue;
            }
            //判定是否是下划线和点
            if(ch == '_' || ch == '.'){
                continue;
            }
            //到了这里发现不合法了，因此校验不通过了，该字符非法
            return false;
        }
        //全部校验通过！
        return true;
    }

    //验证是否能够转发给这个绑定对应的队列
    public static boolean route(ExchangeTtype exchangeType, Bingding bingding, Message message) throws MQException {
        //根据不同exchangeType来判定规则
        if(exchangeType == ExchangeTtype.FINOUT){
            return true;
        }else if(exchangeType == ExchangeTtype.TYPOIC){
            return routeTopic(bingding,message);
        }else{
            //不应该存在！直接交换机我们上层判断过了
            throw new MQException("[RouterUtils] 交换机类型非法！"+exchangeType);
        }
    }

    //主题交换机的转发规则，使用双指针算法
    /*
    1. 例子一：bingdingKey:aaa.bbb.ccc,此时必须要求routingKey和我们的bingdingKey必须完全相同！
    2. 例子二：bingdingKey:aaa.*.ccc，此时routingKey：aaa.bbb.ccc ✓ aaa.b.ccc ✓ aaa.b.b.c ✕
    3. 例子三：bingdingKey:aaa.#.ccc，此时routingKey：aaa.bbb.ccc ✓ aaa.b.b.c ✓ aaa.ccc ✓ aaa.b.b.b ✕
    如果bingdingKey是单个#，则可以匹配所有的routingKey，此时topic交换机就变成了fanout交换机了！
     */
    private static boolean routeTopic(Bingding bingding, Message message) {
        String bingdingKey = bingding.getBindingKey();
        String routingKey = message.getRoutingKey();
        //切分
        String[] bingdingKeySplit = bingdingKey.split("\\.");
        String[] routingKeySplit = routingKey.split("\\.");
        //定义双指针
        int pos1 = 0;
        int pos2 = 0;
        //遍历比较
        while(pos1 < bingdingKeySplit.length && pos2 < routingKeySplit.length){
            //*匹配任意一个字符
            if(bingdingKeySplit[pos1].equals("*")){
                pos1++;
                pos2++;
                continue;
            }
            //#匹配多个字符
            if(bingdingKeySplit[pos1].equals("#")){
                //判断bingdingKeySplit的#下一个位置有没有内容
                pos1++;
                //这个#可以直接把routingKey剩下全部内容匹配走！
                if(pos1 == bingdingKeySplit.length){
                    return true;
                }
                //说明后面还有东西，一直往后找，没找到返回-1
                pos2 = findNextMatch(routingKeySplit,pos2,bingdingKeySplit[pos1]);
                if(pos2 == -1){
                    //没找到匹配的结果
                    return false;
                }
                //继续往后匹配
                pos1++;
                pos2++;
                continue;
            }else{
                //两个个字符完全相等情况
                if(!bingdingKeySplit[pos1].equals(routingKeySplit[pos2])){
                    return false;
                }
                pos1++;
                pos2++;
            }
        }
        //循环结束后看看下标是否都在数组的末尾边界（但凡有一个先到就算失败）
        return pos1 == bingdingKeySplit.length && pos2 == routingKeySplit.length;
    }

    //寻找routingKey的指定字符位置
    private static int findNextMatch(String[] routingKeySplit, int pos, String str) {
        while(pos < routingKeySplit.length && !routingKeySplit[pos].equals(str)){
            pos++;
        }
        return pos == routingKeySplit.length ? -1 : pos;
    }
}
