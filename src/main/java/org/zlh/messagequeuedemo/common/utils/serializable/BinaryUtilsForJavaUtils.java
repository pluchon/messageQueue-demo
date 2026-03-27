package org.zlh.messagequeuedemo.common.utils.serializable;

import java.io.*;

/**
 * @author pluchon
 * @create 2026-03-27-18:15
 * 作者代码水平一般，难免难看，请见谅
 */
//此处我们采用JAVA原生的类进行二进制的序列化，我们的其他JAVA对象都是可以进行适用的（前提是实现serializable接口）！
//为什么不使用JSON呢？因为我们JSON在面对特殊化的字符（比如",{}....）这些会解析错误
// TODO 当然我们可以引入base64编码，用更多的空间进行转码，但是会导致空间变大，而且效率也低
// TODO 序列化的方式不只是可以JAVA标准库，我们可以用Hessian，protobuffer，thirft等等
// TODO serivalversionUID是用来验证我们的版本的，就是说如果我们修改了这个对象的属性内容，进行反序列化时候大概率会抛出异常
public class BinaryUtilsForJavaUtils {
    // 把一个对象序列化为二进制，并且我们的字节数组是变长的
    public static byte[] toBinary(Object o) throws IOException {
        //这个流对象不关也没有关系，因为在内存中只是一个字节数组
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try(ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
            //通过objectOutputStream写入到我们的outputStream中，因此如果我们关联的是哪个流对象会自动进行关联
            objectOutputStream.writeObject(o);
            //取出二进制数据转换成字节数组作为返回
            return outputStream.toByteArray();
        }
    }

    // 把一个二进制对象反序列化为正常对象
    public static Object fromBinary(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        Object o = null;
        try(ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)){
            //读取我们关联的反序列的数据，返回结果就是一个Object对象
            o = objectInputStream.readObject();
            return o;
        }
    }
}
