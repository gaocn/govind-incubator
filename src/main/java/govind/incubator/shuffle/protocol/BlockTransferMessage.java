package govind.incubator.shuffle.protocol;

import govind.incubator.network.protocol.Encodable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * 定义{@link govind.incubator.shuffle.ExternalShuffleBlockHandler}能够处理的
 * 消息消息。定义了如下消息类型：
 * 1、OpenBlock，表示打开某个shuffle文件，返回StreamHandle;
 * 2、UploadBlock，仅仅被NettyBlockTransferService使用；
 * 3、RegisterExecutor，注册Executor；
 */
public abstract class BlockTransferMessage implements Encodable {
	protected abstract Type type();

	/** 需要将消息类型序列化，以方便能够被解序列化出来 */
	public enum Type {
		OPEN_BLOCK(0), UPLOAD_BLOCK(1), REGISTER_EXECUTOR(2), STREAM_HANDLE(3), REGISTER_DRIVER(4);
		private final byte id;

		Type(int id) {
			assert id < 128 : "消息类型不能超过128种";
			this.id = (byte) id;
		}

		public byte id() {
			return this.id;
		}
	}

	/** Java接口中不支持静态方法，因此需要将其定义在静态类中 */
	public static class Decoder {
		/** 根据消息类型解析消息 */
		public static BlockTransferMessage fromByteByffer(ByteBuffer msg) {
			ByteBuf buf = Unpooled.wrappedBuffer(msg);
			byte type = buf.readByte();
			switch (type) {
				case 0:
					return OpenBlock.decode(buf);
				case 1:
					return UploadBlock.decode(buf);
				case 2:
					return RegisterExecutor.decode(buf);
				case 3:
					return StreamHandle.decode(buf);
				case 4:
					return RegisterDriver.decode(buf);
				default:
					throw new IllegalArgumentException("不支持的消息类型：" + type);
			}
		}
	}

	/** 序列化消息：类型+消息本身 */
	public ByteBuffer toByteBuffer() {
		ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
		buf.writeByte(type().id);
		encode(buf);
		assert buf.writableBytes() == 0 : "状态非法：剩余未用空间为" + buf.writableBytes();
		return buf.nioBuffer();
	}
}
