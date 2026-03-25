package org.zlh.messagequeuedemo.mqserver.datacenter;

import org.springframework.beans.factory.annotation.Autowired;
import org.zlh.messagequeuedemo.mqserver.mapper.MetaMapper;

/**
 * @author pluchon
 * @create 2026-03-26-00:35
 * 作者代码水平一般，难免难看，请见谅
 */
// 通过这个类取进行我们数据库的操作
public class DataBaseManager {
    private MetaMapper metaMapper;

    // 对数据库进行初始化->建库建表操作并插入一些默认的数据
    // 如果已经存在库和表，啥都不干，否则我们就建库建表并插入一些默认的数据
    private void init(){

    }
}
