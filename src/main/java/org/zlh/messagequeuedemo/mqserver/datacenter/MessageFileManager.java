package org.zlh.messagequeuedemo.mqserver.datacenter;

import lombok.extern.slf4j.Slf4j;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.common.utils.serializable.BinaryUtilsForJavaUtils;
import org.zlh.messagequeuedemo.mqserver.core.MSGQueue;
import org.zlh.messagequeuedemo.mqserver.core.Message;

import java.io.*;
import java.util.LinkedList;
import java.util.Scanner;

/**
 * @author pluchon
 * @create 2026-03-26-21:03
 *         作者代码水平一般，难免难看，请见谅
 */
// 消息内容文件管理类
// TODO GC垃圾回收拓展，如果我们有效数据也很多，则可以进行文件拆分，拆分小的文件之间可能会进行相互的合并操作
@Slf4j
public class MessageFileManager {
    // 该队列所在的目录
    private String getQueueDir(String queueName) {
        return "./data/" + queueName;
    }

    // 该队列的文件所在的文件路径
    // TODO 二进制文件txt不合适，后续可以改成.bin
    private String getQueueDataPath(String queueName) {
        return getQueueDir(queueName) + "/queue_data.txt";
    }

    // 该队列的消息统计文件路径
    private String getQueueStatPath(String queueName) {
        return getQueueDir(queueName) + "/queue_stat.txt";
    }

    // 定义内部类表示该队列的统计信息
    // 使用静态内部类可以不依赖于外部类的创建，不搞getter与setter方法了
    static public class Stat {
        // 总消息数量
        public int totalCount;
        // 有效消息数量
        public int validCount;
    }

    // 获取Stat文件的属性
    // 由于我们的文件是个文本文件，因此我们可以使用Scanner进行读取
    private Stat getStat(String queueName) {
        Stat stat = new Stat();
        // 写入到内存
        try (InputStream inputStream = new FileInputStream(getQueueStatPath(queueName))) {
            Scanner content = new Scanner(inputStream);
            stat.totalCount = content.nextInt();
            stat.validCount = content.nextInt();
            return stat;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 设置Stat文件的属性，注意我们OutputStream打开文件默认会把源文件内容清空！
    // 会把我们新的内容覆盖给旧的内容，把第二个参数设置为true表示我们把新的内容追加到末尾
    private void setStat(String queueName, Stat stat) {
        // 写出到文件
        try (OutputStream outputStream = new FileOutputStream(getQueueStatPath(queueName))) {
            PrintWriter printWriter = new PrintWriter(outputStream);
            printWriter.write(stat.totalCount + "\t" + stat.validCount);
            // 从缓冲区刷入到硬盘中
            printWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 创建队列对应的目录与文件
    public void createQueue(String queueName) throws IOException {
        File queueFir = new File(getQueueDir(queueName));
        // 不存在才进行队列目录的创建
        if (!queueFir.exists()) {
            if (!queueFir.mkdirs()) {
                throw new IOException("[MessageFileManager] 目录创建失败：" + queueFir.getAbsoluteFile());
            }
        }
        // 创建队列数据文件要用createNewFile
        File queueDataFir = new File(getQueueDataPath(queueName));
        if (!queueDataFir.exists()) {
            if (!queueDataFir.createNewFile()) {
                throw new IOException("[MessageFileManager] 数据文件创建失败：" + queueDataFir.getAbsoluteFile());
            }
        }
        // 创建消息统计文件
        File queueStatFir = new File(getQueueStatPath(queueName));
        if (!queueStatFir.exists()) {
            if (!queueStatFir.createNewFile()) {
                throw new IOException("[MessageFileManager] 队列统计文件创建失败：" + queueStatFir.getAbsoluteFile());
            }
        }
        // 给消息统计文件设定初始值
        Stat stat = new Stat();
        stat.validCount = stat.totalCount = 0;
        setStat(queueName, stat);
    }

    // 删除队列对应的目录与文件，和上面的创建对应
    public void deleteQueue(String queueName) throws IOException {
        // 先删除文件再删除目录
        File queueDataFir = new File(getQueueDataPath(queueName));
        boolean ok1 = queueDataFir.delete();
        File queueStatFir = new File(getQueueStatPath(queueName));
        boolean ok2 = queueStatFir.delete();
        File queueDir = new File(getQueueDir(queueName));
        boolean ok3 = queueDir.delete();
        // 只要有一个失败了就说明删除不成功
        if (!ok1 || !ok2 || !ok3) {
            throw new IOException("删除队列目录与文件失败" + queueDir.getAbsoluteFile());
        }
    }

    // 针对文件以及目录是否存在进行检查
    // 针对于消息是否要持久化的辅助检查方法
    private boolean checkFileExists(String queueName) {
        File queueDataFile = new File(getQueueDataPath(queueName));
        if (!queueDataFile.exists()) {
            return false;
        }
        File queueStatFile = new File(getQueueStatPath(queueName));
        if (!queueStatFile.exists()) {
            return false;
        }
        // 都存在返回true
        return true;
    }

    // 把新的消息放到对应的队列文件中
    public void sendMessage(MSGQueue queue, Message message) throws MQException, IOException {
        // 队列对应的名称
        String queueName = queue.getName();
        // 检查参数合法性（存在性问题）
        if (!checkFileExists(queueName)) {
            // 文件不存在，当前写入操作无意义
            throw new MQException("[MessageFileManager] 队列对应的文件不存在：" + queue.getName());
        }
        // 对象序列化（前提是实现了序列化接口）
        byte[] messageBinary = BinaryUtilsForJavaUtils.toBinary(message);
        // 针对线程安全，我们队列进行加锁，不同线程写同一个队列要阻塞等待
        // 提示警告是因为idea不确定你这个方法能否达到预期效果且有效（不同线程对同一个对象加锁，因为idea怕的是传的是不同的队列对象）
        // 但是没关系我们以后是传入我们内存里管理的queue对象
        synchronized (queue) {
            // 获取当前队列数据文件的长度，用来计算该消息的offsetBeg和offsetEnd！
            // 以便于我们把新的message数据写入到我们队列数据文件的末尾
            // 此时offsetBeg就是我们当前 文件长度+4，而我们的offsetEnd就是我们 文件长度+4+消息长度
            // +4是我们约定的格式，用四个字节表示我们的消息长度
            // 获取文件对象
            File file = new File(getQueueDataPath(queueName));
            // 获取长度，单位字节
            long length = file.length();
            // 设置长度
            message.setOffsetBeg(length + 4);
            message.setOffsetEnd(length + 4 + messageBinary.length);
            // 写入文件，打开文件，而且注意我们写入不是覆盖而是追加到文件内容末尾，因此多加参数true
            try (OutputStream outputStream = new FileOutputStream(file, true)) {
                // 先规定我们写入消息的offsetBrg属性和offsetEnd长度属性，占据4个字节
                // 虽然我们都write方法有一个int类型参数（4个字节），但是实际上只能每一次写入一个字节，也就是Byte参数！
                // 也可以四个字节分别取出来再逐个字节写入，使用位运算(num = 0xaabbccdd) -->num & 0xff = dd,(num >> 8) &
                // 0xff => cc .....
                // 但是我们标准库封装了，因此我们使用Java标准库
                try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                    // 写入int四个字节，表示这个消息有多长
                    dataOutputStream.writeInt(messageBinary.length);
                    // 写入消息本体，就是字节数组内容本体
                    dataOutputStream.write(messageBinary);
                }
            }
            // 更新消息统计文件
            Stat stat = getStat(queueName);
            stat.totalCount += 1;
            stat.validCount += 1;
            // 重新写入对象
            setStat(queueName, stat);
        }
    }

    // 删除消息，软删除（不是真的删除）
    // 注意我们是随机访问文件中内容，不能使用inputStream和outputStream，因为它们都是从头或者是尾开始读
    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException {
        // 注意加锁！！
        synchronized (queue) {
            String queueName = queue.getName();
            // 里面的seek方法是定位的鼠标光标位置，参数是文件位置和打开方式(rw->可读可写)
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(getQueueDataPath(queueName), "rw")) {
                // 数组内容大小我们可以直接计算
                byte[] messageInfoByte = new byte[(int) (message.getOffsetEnd() - message.getOffsetBeg())];
                // 让光标移动到该条消息起始位置
                randomAccessFile.seek(message.getOffsetBeg());
                // 把messageInfo这个空间读取满，全部写入到字节数组中
                randomAccessFile.read(messageInfoByte);
                // 二进制数据转换成对象
                Message messageInfo = (Message) BinaryUtilsForJavaUtils.fromBinary(messageInfoByte);
                // 修改状态
                // 对于我们在内存中管理的message对象，我们刚刚修改的是文件的状态
                // 针对内存的消息设置无所谓了，因为这个参数是专门表示文件中表示的状态，后续我们马上就会从内存中进行销毁
                messageInfo.setIsVaild((byte) 0x0);
                // 写回文件，进行覆盖
                byte[] newMessageByte = BinaryUtilsForJavaUtils.toBinary(messageInfo);
                // 往我们指定的位置写入，注意我们要再重新定义光标位置（因为我们在读写过程中我们光标位置会进行变化，因此要重新定位）
                randomAccessFile.seek(message.getOffsetBeg());
                randomAccessFile.write(newMessageByte);
                // 我们这通操作只有一个字节发生改变，但是非常重要
            }
            // 更新统计文件
            Stat stat = getStat(queueName);
            // >0才可以减一
            if (stat.validCount > 0) {
                stat.validCount -= 1;
            }
            // 重新写入
            setStat(queueName, stat);
        }
    }

    // 加载文件中所有的消息并放入内存中，这个方法在程序启动的时候进行调用
    // 使用LinkedList是为了后续进行头部删除操作
    // 我们传入queueName表示我们不进行加锁，因为我们这个方法在程序启动的时候调用，不涉及多线程的调用
    public LinkedList<Message> queryAllMessage(String queueName)
            throws IOException, MQException, ClassNotFoundException {
        LinkedList<Message> messageLinkedList = new LinkedList<>();
        // 打开文件读取数据，必须是isVaild有效才可以，按顺序读取使用流
        try (InputStream inputStream = new FileInputStream(getQueueDataPath(queueName))) {
            // 先读四个字节获取到该消息的信息，才可以正确往后读
            try (DataInputStream dataInputStream = new DataInputStream(inputStream)) {
                // 我们需要读取多次
                long currentOffset = 0L;
                while (true) {
                    // 读取我们的该消息的长度
                    int messageLength = dataInputStream.readInt();
                    // 如何判断我们读取到了文件末尾？我们可以根据dataInputStream的messageLength判断(EOF异常)
                    // 按照这个长度读取消息内容
                    byte[] buffer = new byte[messageLength];
                    // 读取指定长度内容
                    int readLength = dataInputStream.read(buffer);
                    // 判断长度是否正确
                    if (readLength != messageLength) {
                        // 该消息有问题，或者是文件有问题，可能是格式错乱
                        throw new MQException("[MessageFileManager] 消息或文件格式错误！" + queueName);
                    }
                    // 转换成对象并填入我们的LinkedList
                    Message messageInfo = (Message) BinaryUtilsForJavaUtils.fromBinary(buffer);
                    // 判定消息对象是不是无效对象，有效消息才可以放入
                    if (messageInfo.getIsVaild() != 0x1) {
                        // offset记得要更新，因为就算是无效消息也占着位置
                        currentOffset += messageLength + 4;
                        continue;
                    }
                    // 填写offsetBeg和offsetEnd，也就是我们当前的光标位置（我们手动记录），我们DataInputStream不方便直接获取到文件光标位置
                    // 注意我们要+4，因为我们开始读了四个字节
                    currentOffset += 4;
                    messageInfo.setOffsetBeg(currentOffset);
                    // 加上消息长度
                    currentOffset += messageLength;
                    messageInfo.setOffsetEnd(currentOffset);
                    // 加入到LinkedList中
                    messageLinkedList.add(messageInfo);
                }
            } catch (EOFException e) {
                // 注意此处catch不是处理异常，而是处理文件读取到末尾的标志
                // 这个catch无需做啥特殊处理，自动读取到这里循环就会正常结束了
                log.info("[MessageFileManager] 读取消息文件数据完成！");
            }
        }
        return messageLinkedList;
    }

    // 检查是否要对当前队列进行垃圾回收
    // 总消息数 > 2000,有效消息 < 50%
    public boolean checkGC(String queueName) {
        Stat stat = getStat(queueName);
        return stat.totalCount > 2000 && (double) stat.validCount / stat.totalCount < 0.5;
    }

    // 回收后新文件所在的位置
    private String queueNewPath(String queueName) {
        return getQueueDir(queueName) + "/queue_data_new.txt";
    }

    // GC垃圾回收，使用复制算法进行（加锁，因为是对我们文件的清洗！！）
    public void GC(MSGQueue queue) throws MQException, IOException, ClassNotFoundException {
        synchronized (queue) {
            String queueName = queue.getName();
            long gcStart = System.currentTimeMillis();
            // 创建一个新文件
            File newQueueFile = new File(queueNewPath(queueName));
            // 查看是否存在，如果存在说明上一次GC存在残留文件，也就是说该文件不该存在
            if (newQueueFile.exists()) {
                throw new MQException("[MessageFileManager] GC发现该队列的queue_data_new.txt已经存在" + queueName);
            }
            // 创建文件
            boolean isOk = newQueueFile.createNewFile();
            // 查看结果
            if (!isOk) {
                throw new MQException(
                        "[messageFileManager] 创建新队列文件'queue_data_new.txt'失败 -> " + newQueueFile.getAbsoluteFile());
            }
            // 从旧文件中读取所有的有效消息对象，之前定义过了
            LinkedList<Message> messageLinkedList = queryAllMessage(queueName);
            // 进行复制算法，不复用sendMessage方法了
            try (OutputStream outputStream = new FileOutputStream(newQueueFile)) {
                try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                    for (Message messageInfo : messageLinkedList) {
                        byte[] binaryMessage = BinaryUtilsForJavaUtils.toBinary(messageInfo);
                        // 先写四个字节消息长度（必须用writeInt，和sendMessage/queryAllMessage保持格式一致！）
                        dataOutputStream.writeInt(binaryMessage.length);
                        // 写入消息本体
                        dataOutputStream.write(binaryMessage);
                    }
                }
            }
            // 删除旧的文件，并把新文件重命名
            File queueOldFile = new File(getQueueDataPath(queueName));
            isOk = queueOldFile.delete();
            // 判断删除是否成功
            if (!isOk) {
                throw new MQException("[MessageFileManager] 旧队列文件删除失败 -> " + queueOldFile.getAbsoluteFile());
            }
            // 重命名我们新的队列数据文件，使用的我们的旧队列名字，便于后续业务进行展开！
            isOk = newQueueFile.renameTo(queueOldFile);
            if (!isOk) {
                throw new MQException("[MessageFileManager] 文件重命名失败 -> " + newQueueFile.getAbsoluteFile() + "≠"
                        + queueOldFile.getAbsoluteFile());
            }
            // 更新我们统计文件信息
            Stat stat = getStat(queueName);
            stat.totalCount = stat.validCount = messageLinkedList.size();
            // 写回文件
            setStat(queueName, stat);
            // GC正式结束
            long gcEnd = System.currentTimeMillis();
            // 计算耗时
            log.info("[MessageFileManager] GC执行完毕：{}，耗时：{}ms", queueName, gcEnd - gcStart);
        }
    }
}