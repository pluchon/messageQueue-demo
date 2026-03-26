package org.zlh.messagequeuedemo.mqserver.datacenter;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Scanner;

/**
 * @author pluchon
 * @create 2026-03-26-21:03
 * 作者代码水平一般，难免难看，请见谅
 */
//消息内容文件管理类
// TODO GC垃圾回收拓展，如果我们有效数据也很多，则可以进行文件拆分，拆分小的文件之间可能会进行相互的合并操作
@Slf4j
public class MessageFileManager {
    //该队列所在的目录
    private String getQueueDir(String queueName){
        return "./data/"+queueName;
    }

    //该队列的文件所在的文件路径
    // TODO 二进制文件txt不合适，后续可以改成.bin
    private String getQueueDataPath(String queueName){
        return getQueueDir(queueName)+"/queue_data.txt";
    }

    //该队列的消息统计文件路径
    private String getQueueStatPath(String queueName){
        return getQueueDir(queueName)+"/queue_stat.txt";
    }

    //定义内部类表示该队列的统计信息
    //使用静态内部类可以不依赖于外部类的创建，不搞getter与setter方法了
    static public class Stat{
        //总消息数量
        public Integer totalCount;
        //有效消息数量
        public Integer validCount;
    }

    //获取Stat文件的属性
    //由于我们的文件是个文本文件，因此我们可以使用Scanner进行读取
    private Stat getStat(String queueName){
        Stat stat = new Stat();
        //写入到内存
        try(InputStream inputStream = new FileInputStream(getQueueStatPath(queueName))) {
            Scanner content = new Scanner(inputStream);
            stat.totalCount = content.nextInt();
            stat.validCount = content.nextInt();
            return stat;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //设置Stat文件的属性，注意我们OutputStream打开文件默认会把源文件内容清空！
    //会把我们新的内容覆盖给旧的内容，把第二个参数设置为true表示我们把新的内容追加到末尾
    private void setStat(String queueName,Stat stat){
        //写出到文件
        try(OutputStream outputStream = new FileOutputStream(getQueueStatPath(queueName))) {
            PrintWriter printWriter = new PrintWriter(outputStream);
            printWriter.write(stat.totalCount+"\t"+stat.validCount);
            //从缓冲区刷入到硬盘中
            printWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //创建队列对应的目录与文件
    public void createQueue(String queueName) throws IOException {
        File queueFir = new File(getQueueDir(queueName));
        //不存在才进行队列目录的创建
        if(!queueFir.exists()){
            if(!queueFir.mkdirs()){
                throw new IOException("[MessageFileManager] 目录创建失败："+queueFir.getAbsoluteFile());
            }
        }
        //创建队列数据文件
        File queueDataFir = new File(getQueueDataPath(queueName));
        if(!queueDataFir.exists()){
            if(!queueDataFir.mkdirs()){
                throw new IOException("[MessageFileManager] 数据文件创建失败："+queueDataFir.getAbsoluteFile());
            }
        }
        //创建消息统计文件
        File queueStatFir = new File(getQueueStatPath(queueName));
        if(!queueStatFir.exists()){
            if(!queueStatFir.mkdirs()){
                throw new IOException("[MessageFileManager] 队列数据文件创建失败："+queueStatFir.getAbsoluteFile());
            }
        }
        //给消息统计文件设定初始值
        Stat stat = new Stat();
        stat.validCount = stat.totalCount = 0;
        setStat(queueName,stat);
    }

    //删除队列对应的目录与文件，和上面的创建对应
    public void deleteQueue(String queueName) throws IOException {
        //先删除文件再删除目录
        File queueDataFir = new File(getQueueDataPath(queueName));
        boolean ok1 = queueDataFir.delete();
        File queueStatFir = new File(getQueueStatPath(queueName));
        boolean ok2 = queueStatFir.delete();
        File queueDir = new File(getQueueDir(queueName));
        boolean ok3 = queueDir.delete();
        //只有都成功了才删除成功
        if(ok1 || ok2 || ok3){
            throw new IOException("删除队列目录与文件失败"+queueDir.getAbsoluteFile());
        }
    }

    //针对文件以及目录是否存在进行检查
    //针对于消息是否要持久化的辅助检查方法
    private boolean checkFileExists(String queueName){
        File queueDataFile = new File(getQueueDataPath(queueName));
        if(!queueDataFile.exists()){
            return false;
        }
        File queueStatFile = new File(getQueueStatPath(queueName));
        if(!queueStatFile.exists()){
            return false;
        }
        //都存在返回true
        return true;
    }
}
