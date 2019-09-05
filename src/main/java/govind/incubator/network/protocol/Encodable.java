package govind.incubator.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 支持将数组编码到ByteBuf中。
 *
 * NOTES：
 * （1）实现Encodable接口的类，需要实现一个静态的decode(ByteBuf)方法，
 * 用于解码ByteBuf中的二进制数据为对应的对象，在解码时被调用。
 * （2）、解码过程中，如果对象使用了ByteBuf中的内容(而不是通过拷贝方式)，则
 * 需要显示调用{@link ByteBuf#retain()}方法。
 * （3）、每次新增一个消息类型需要在{@link Message.Type}中添加！
 */
public interface Encodable {

	/**
	 * 消息对象编码后的长度
	 * @return
	 */
	int encodedLength();

	/**
	 * 将当前消息对象序列化到ByteBuf中，写入字节数必须与{@link #encodedLength()}返回值相等
	 * @param buf
	 */
	void encode(ByteBuf buf);

}
