package org.zlh.messagequeuedemo.mqclient.client;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.zlh.messagequeuedemo.common.exception.MQException;
import org.zlh.messagequeuedemo.common.utils.serializable.BinaryUtilsForJavaUtils;
import org.zlh.messagequeuedemo.mqclient.dto.Request;
import org.zlh.messagequeuedemo.mqclient.vo.BasicReturns;
import org.zlh.messagequeuedemo.mqclient.vo.Response;
import org.zlh.messagequeuedemo.mqclient.vo.SubScribeReturns;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author pluchon
 * @create 2026-03-31-14:24
 * 作者代码水平一般，难免难看，请见谅
 */
//一个TCP连接内部管理多个方法
@Getter
@Setter
@Slf4j
public class Connect {
    //网络通信
    private Socket socket = null;
    //channel管理，使用哈希表key->channelID，value->channel对象
    private ConcurrentHashMap<String,Channel> channelConcurrentHashMap = new ConcurrentHashMap<>();
    //流对象
    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;

    //处理回调的线程池
    private ExecutorService callbackPool = null;

    public Connect(String host, int port) throws IOException {
        socket = new Socket(host,port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        dataInputStream = new DataInputStream(inputStream);
        dataOutputStream = new DataOutputStream(outputStream);
        callbackPool = Executors.newFixedThreadPool(4);

        //创建扫描线程，不停的从socket读取数据，交给对应的channel处理
        Thread t = new Thread(()->{
                try {
                    while (!socket.isClosed()) {
                        Response response = getResponse();
                        dispatchResponse(response);
                    }
                } catch (SocketException e) {
                    log.info("[Connect] 连接正常断开！");
                } catch (IOException | ClassNotFoundException | MQException e){
                    log.error("[Connect] 连接异常断开！");
                }
        });
        t.start();
    }

    //使用这个方法处理当前的响应是控制请求响应还是服务器推送到响应
    private void dispatchResponse(Response response) throws IOException, ClassNotFoundException, MQException {
        if(response.getType() == 0xc){
            //服务器推送到消息数据
            SubScribeReturns subScribeReturns = (SubScribeReturns) BinaryUtilsForJavaUtils.fromBinary(response.getPayload());
            //执行对应的回调方法，找到channel对象，执行该channel对象的回调方法
            Channel channel = channelConcurrentHashMap.get(subScribeReturns.getChannelId());
            if(channel == null){
                throw new MQException("[Connect] 该消息对应的channel在客户端不存在！");
            }
            //交给线程池
            callbackPool.submit(()->{
                try {
                    channel.getConsumer().handleDelivery(subScribeReturns.getConsumerTag(),subScribeReturns.getBasicProperties()
                    ,subScribeReturns.getContent());
                } catch (MQException | IOException e) {
                    log.error("[Connect] 线程池处理回调失败！");
                }
            });
        }else{
            //针对控制请求的响应
            BasicReturns returns = (BasicReturns) BinaryUtilsForJavaUtils.fromBinary(response.getPayload());
            //把结果放入对应的channel的收集服务器响应的哈希表中
            Channel channel = channelConcurrentHashMap.get(returns.getChannelId());
            if(channel == null){
                throw new MQException("[Connect] 该消息对应的channel在客户端不存在！");
            }
            channel.putReturns(returns);
        }
    }

    //发送请求
    public void writeRequest(Request request) throws IOException {
        dataOutputStream.writeInt(request.getType());
        dataOutputStream.writeInt(request.getLength());
        dataOutputStream.write(request.getPayload());
        dataOutputStream.flush();
        log.info("[Connect] 发送请求！ type={} length={}",request.getType(),request.getLength());
    }

    //读取响应
    public Response getResponse() throws IOException {
        Response response = new Response();
        response.setType(dataInputStream.readInt());
        response.setLength(dataInputStream.readInt());
        byte[] payload = new byte[response.getLength()];
        int read = dataInputStream.read(payload);
        if(read != response.getLength()){
            throw new IOException("[Connect] 读取到相应的数据不完整！");
        }
        response.setPayload(payload);
        log.info("[Connect] 收到响应！ type={} length={}",response.getType(),response.getLength());
        return response;
    }

    //创建channel，在connect创建一个channel
    public Channel createChannel() throws IOException {
        //加入C-前缀便于区分我的UUID，表示属于我们的Channel
        String channelId = "C-"+UUID.randomUUID().toString();
        Channel channel = new Channel(channelId,this);
        //放入表，便于集中管理
        channelConcurrentHashMap.put(channelId,channel);
        //同时把创建channel告知服务器，交给channel类
        boolean isOk = channel.createChannel();
        //服务器创建失败了，整个这一次创建操作不通过，把刚刚加入哈希表的键值对删了
        if(!isOk){
            channelConcurrentHashMap.remove(channelId);
            return null;
        }
        return channel;
    }

    //关闭connect，释放资源
    public void close(){
        try {
            callbackPool.shutdownNow();
            channelConcurrentHashMap.clear();
            outputStream.close();
            inputStream.close();
            //其他两个data开头的自然就会关闭
            socket.close();
        } catch (IOException e) {
            log.error("[Connect] 流对象关闭异常！");
        }
    }
}
