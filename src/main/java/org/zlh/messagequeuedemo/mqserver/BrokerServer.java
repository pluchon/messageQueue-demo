package org.zlh.messagequeuedemo.mqserver;

import lombok.extern.slf4j.Slf4j;
import org.zlh.messagequeuedemo.common.constant.ConstantForBrokerServer;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.common.utils.serializable.BinaryUtilsForJavaUtils;
import org.zlh.messagequeuedemo.mqclient.dto.Request;
import org.zlh.messagequeuedemo.mqclient.dto.arguments.BasicArguments;
import org.zlh.messagequeuedemo.mqclient.dto.arguments.virtualhost.*;
import org.zlh.messagequeuedemo.mqclient.vo.BasicReturns;
import org.zlh.messagequeuedemo.mqclient.vo.Response;
import org.zlh.messagequeuedemo.mqclient.vo.SubScribeReturns;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author pluchon
 * @create 2026-03-31-10:45
 *         作者代码水平一般，难免难看，请见谅
 */
// 一个TCP服务器，也就是我们的消息队列本体服务器，里面可以有多个channel
@Slf4j
public class BrokerServer {
    private ServerSocket serverSocket = null;

    // 当前只考虑一个虚拟主机！
    // TODO 后续扩展多个虚拟主机
    private VirtualHost virtualHost = new VirtualHost(ConstantForBrokerServer.VIRTUAL_HOST_NAME);

    // 使用这个哈希表表示当前所有会话，表示有哪些客户端和我们的服务器进行通信！
    // Key->channelID，Value->会话对象
    private ConcurrentHashMap<String, Socket> sessions = new ConcurrentHashMap<>();

    // 处理多个客户端请求，引入线程池
    private ExecutorService executorService = null;

    // 引入变量，控制服务器是否继续运行，加入volatile处理多线程
    private volatile boolean isRun = true;

    // 构造方法，指定绑定端口号
    public BrokerServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    // 启动服务器
    public void start() throws IOException {
        log.info("[BrokerServer] 服务器启动中......");
        // 不显示的设定线程上线
        executorService = Executors.newCachedThreadPool();
        // 处理每个客户端情况
        while (isRun) {
            Socket clientSocket = serverSocket.accept();
            // 获取到的客户端连接丢入线程池
            executorService.submit(() -> {
                processConnection(clientSocket);
            });
        }
    }

    // 停止服务器，直接杀掉进程，但是此处搞一个停止方法，用户后续单元测试
    public void stop() throws IOException {
        // 停止循环
        isRun = false;
        // 把线程池任务都清掉，同时让线程销毁
        executorService.shutdownNow();
        // 关闭socket连接
        serverSocket.close();
    }

    // 处理一个客户端的连接
    // 可能会涉及到多个请求和响应
    private void processConnection(Socket clientSocket) {
        // 获取流对象
        try (InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream()) {
            // 为了按照我们特定格式的协议，因此就需要用到data的stream流
            try (DataInputStream dataInputStream = new DataInputStream(inputStream);
                    DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                // 读到EOF直接抛出异常，结束循环
                while (true) {
                    // 读取请求并解析
                    Request request = readRequest(dataInputStream);
                    // 根据请求计算响应
                    Response response = process(request, clientSocket);
                    // 把响应写回给客户端
                    writeResponse(dataOutputStream, response);
                }
            }
        } catch (EOFException | SocketException e) {
            log.info("[BrokerServer] 连接关闭！客户端IP->{}->{}", serverSocket.getInetAddress().toString(),
                    clientSocket.getPort());
        } catch (IOException | MQException | ClassNotFoundException e) {
            log.error("[BrokerServer] 连接出现异常！");
        } finally {
            // 关闭socket对象
            // 同时一个TCP可能包含多个channel，因此要把当前链接的channel都清理
            try {
                clientSocket.close();
                clearClosedSession(clientSocket);
            } catch (IOException e) {
                log.info("[BrokerServer] 连接关闭成功！");
            }
        }
    }

    // 读取请求并解析
    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        Request request = new Request();
        // 读取请求类型
        request.setType(dataInputStream.readInt());
        // 读取请求长度
        request.setLength(dataInputStream.readInt());
        // 读取length字节剩余内容
        byte[] payload = new byte[request.getLength()];
        int read = dataInputStream.read(payload);
        if (read != request.getLength()) {
            throw new IOException("[BrokerServer] 读取请求格式出错！");
        }
        request.setPayload(payload);
        return request;
    }

    // 根据请求类型计算不同的响应
    private Response process(Request request, Socket clientSocket)
            throws IOException, ClassNotFoundException, MQException {
        // 把request的负载做一个初步的解析，注意我们里面放的内容是不一定一样的
        // 比如 type->0x3 => exchangedeclare
        // 但是它们都是继承自basicArgument，因此先转换成它
        BasicArguments basicArguments = (BasicArguments) BinaryUtilsForJavaUtils.fromBinary(request.getPayload());
        // 获取Rid和ChannelId
        log.info("[BrokerServer] rid={},channelId={} type={} length={}", basicArguments.getRid(),
                basicArguments.getChannelId(), request.getType(), request.getLength());
        // 根据type类型进一步区分这一次请求要干什么？
        /*
         * 0x1 创建 channel
         * 0x2 关闭 channel
         * 0x3 创建 exchange
         * 0x4 销毁 exchange
         * 0x5 创建 queue
         * 0x6 销毁 queue
         * 0x7 创建 binding
         * 0x8 销毁 binding
         * 0x9 发送 message
         * 0xa 订阅 message
         * 0xb 返回 ack
         * 0xc 服务器给客⼾端推送的消息. (被订阅的消息) 响应独有的.
         */
        boolean isOk = true;
        if (request.getType() == 0x1) {
            sessions.put(basicArguments.getChannelId(), clientSocket);
            log.info("[BrokerServer] channel创建完成！channelId->{}", basicArguments.getChannelId());
        } else if (request.getType() == 0x2) {
            sessions.remove(basicArguments.getChannelId());
            log.info("[BrokerServer] channel删除完成！channelId->{}", basicArguments.getChannelId());
        } else if (request.getType() == 0x3) {
            // 此时payload就是一个exchangeDeclareArgument对象，强转为本来的对象
            ExchangeDeclareArguments arguments = (ExchangeDeclareArguments) basicArguments;
            isOk = virtualHost.exchangeDeclare(arguments.getExchangeName(), arguments.getExchangeTtype(),
                    arguments.isPermanet(), arguments.isDelete(), arguments.getArguments());
            log.info("[BrokerServer] 交换机创建完成！{}", arguments.getExchangeName());
        } else if (request.getType() == 0x4) {
            ExchangeDeleteArguments arguments = (ExchangeDeleteArguments) basicArguments;
            isOk = virtualHost.exchangeDelete(arguments.getExchangeName());
            log.info("[BrokerServer] 交换机删除完成！{}", arguments.getExchangeName());
        } else if (request.getType() == 0x5) {
            QueueDeclareArguments arguments = (QueueDeclareArguments) basicArguments;
            isOk = virtualHost.queueDeclare(arguments.getQueueName(), arguments.isPermanet(), arguments.isExclusive(),
                    arguments.isDelete(), arguments.getArguments());
            log.info("[BrokerServer] 队列创建完成！{}", arguments.getQueueName());
        } else if (request.getType() == 0x6) {
            QueueDeleteArguments arguments = (QueueDeleteArguments) basicArguments;
            isOk = virtualHost.queueDelete(arguments.getQueueName());
            log.info("[BrokerServer] 队列删除完成！{}", arguments.getQueueName());
        } else if (request.getType() == 0x7) {
            QueueBingdingDeclareArguments arguments = (QueueBingdingDeclareArguments) basicArguments;
            isOk = virtualHost.bingdingDeclare(arguments.getQueueName(), arguments.getExchangeName(),
                    arguments.getBingdingKey());
            log.info("[BrokerServer] 绑定关系创建完成！{}->{}", arguments.getQueueName(), arguments.getExchangeName());
        } else if (request.getType() == 0x8) {
            QueueBingdingDeleteArguments arguments = (QueueBingdingDeleteArguments) basicArguments;
            isOk = virtualHost.bingdingDelete(arguments.getQueueName(), arguments.getExchangeName());
            log.info("[BrokerServer] 绑定关系删除完成！{}->{}", arguments.getQueueName(), arguments.getExchangeName());
        } else if (request.getType() == 0x9) {
            BasicPublishArguments arguments = (BasicPublishArguments) basicArguments;
            isOk = virtualHost.basicPublish(arguments.getExchangeName(), arguments.getRoutingKey(),
                    arguments.getBasicProperties(), arguments.getContent());
            log.info("[BrokerServer] 消息发送成功！{}", arguments.getExchangeName());
        } else if (request.getType() == 0xa) {
            BasicConsumeArguments arguments = (BasicConsumeArguments) basicArguments;
            isOk = virtualHost.basicConsume(arguments.getConsumerTag(), arguments.getQueueName(), arguments.isAutoAck(),
                    (consumerType, basicProperties, body) -> {
                        // 把服务器收到的消息直接推送回对应的消费者客户端
                        // 直到当前收到的消息发给哪个客户端，我们可以利用consumerTag其实是channelId，去session查询得到socket对象
                        // 再往里面发送数据
                        Socket clientSockets = sessions.get(consumerType);
                        // 发现连接已经断开了
                        if (clientSockets == null || clientSockets.isClosed()) {
                            throw new MQException("[BrokerServer] 订阅消息的客户端已经关闭");
                        }
                        // 我们之前约定好的，也就是服务器给客户端推送的消息数据
                        SubScribeReturns subScribeReturns = new SubScribeReturns();
                        subScribeReturns.setConsumerTag(consumerType);
                        subScribeReturns.setBasicProperties(basicProperties);
                        subScribeReturns.setContent(body);
                        // 设置其父类属性！
                        subScribeReturns.setChannelId(consumerType);
                        // 当服务器第一次提示订阅成功后，服务器会源源不断响应消息，此时没有请求进来，因此我们RID就没有作用了
                        // 我们RID本质是第一次请求订阅队列，保证请求和服务器的响应对应上
                        subScribeReturns.setRid("");
                        subScribeReturns.setOk(true);
                        // 序列化
                        byte[] payload = BinaryUtilsForJavaUtils.toBinary(subScribeReturns);
                        // 构造响应数据
                        Response response = new Response();
                        response.setType(0xc);
                        response.setLength(payload.length);
                        // 此处，就是我们的subScribeReturns
                        response.setPayload(payload);
                        // 写入到响应，注意getOutputStream不能关闭，因为我们后需要持续响应！
                        DataOutputStream dataOutputStream = new DataOutputStream(clientSockets.getOutputStream());
                        writeResponse(dataOutputStream, response);
                    });
        } else if (request.getType() == 0xb) {
            BasicAckArguments arguments = (BasicAckArguments) basicArguments;
            isOk = virtualHost.basicAck(arguments.getQueueName(), arguments.getMessageId());
            log.info("[BrokerServer] 主动应答成功！{}->{}", arguments.getQueueName(), arguments.getMessageId());
        } else {
            throw new MQException("[BrokerServer] 未定义的请求类型！" + request.getType());
        }
        // 构造响应负载
        BasicReturns basicReturns = new BasicReturns();
        basicReturns.setChannelId(basicArguments.getChannelId());
        basicReturns.setRid(basicArguments.getRid());
        basicReturns.setOk(isOk);
        // 序列化响应负载
        byte[] payload = BinaryUtilsForJavaUtils.toBinary(basicReturns);
        // 构造响应
        Response response = new Response();
        response.setType(request.getType());
        response.setLength(payload.length);
        response.setPayload(payload);
        log.info("[BrokerServer] 响应构造完成！{}->{},{}->{}", basicReturns.getRid(), basicReturns.getChannelId(),
                response.getType(), response.getLength());
        // 返回
        return response;
    }

    // 把响应写回给客户端
    private void writeResponse(DataOutputStream dataOutputStream, Response response) throws IOException {
        dataOutputStream.writeInt(response.getType());
        dataOutputStream.writeInt(response.getLength());
        dataOutputStream.write(response.getPayload());
        // 刷新缓冲区！保证当前写的数据离开内存！
        dataOutputStream.flush();
    }

    // 把当前链接的channel都清理，遍历sessions把被关闭的socket的键值对删掉
    // 客户端可能在TCP断开了但是channel没断开，这样的键值对应该要被删除，因此我们在finally中留了一手
    private void clearClosedSession(Socket clientSocket) {
        // 遍历哈希表，找到哪个value是我们当前的clientSocket
        List<String> deleteChannelId = new ArrayList<>();
        for (Map.Entry<String, Socket> e : sessions.entrySet()) {
            if (e.getValue() == clientSocket) {
                // 不能直接删除！因为不可以一边遍历一边删除，会导致迭代器失效！
                // 因此把要删除的结果存起来
                deleteChannelId.add(e.getKey());
            }
        }
        // 再来遍历删除！
        for (String channelId : deleteChannelId) {
            sessions.remove(channelId);
        }
        log.info("[BrokerServer] channel连接清理完成！{}", deleteChannelId);
    }
}
