package govind.incubator.network.sasl;

import govind.incubator.network.buffer.NettyManagedBuffer;
import govind.incubator.network.protocol.AbstractMessage;
import govind.incubator.network.util.CodecUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-30
 *
 * Encodes a Sasl-related message which is attempting to authenticate
 * using some credentials tagged with the given appId. This appId allows
 * a single SaslRpcHandler to multiplex different applications which may
 * be using different sets of credentials.
 *
 */
public class SaslMessage extends AbstractMessage {
	/**
	 * serialization tag used to catch incorrect payloads
	 */
	private static final byte TAG_BYTE = (byte) 0xEA;

	public final String appId;

	public SaslMessage(String appId, byte[] message) {
		this(appId, Unpooled.wrappedBuffer(message));
	}

	public SaslMessage(String appId, ByteBuf buf) {
		super(new NettyManagedBuffer(buf),true);
		this.appId = appId;
	}

	@Override
	public Type type() {
		return Type.User;
	}

	@Override
	public int encodedLength() {
		/**
		 * 这里的body size实际上没有用到，尽管这里同样将帧长度编码到
		 * 二进制了流中，这是为了与使用了{@link CodecUtil.ByteArray}
		 * 的不同版本的 RpcRequest保持后向兼容。
		 */
		return 1 + CodecUtil.Strings.encodedLength(appId) +  4;
	}

	@Override
	public void encode(ByteBuf buf) {
		buf.writeByte(TAG_BYTE);
		CodecUtil.Strings.encode(buf, appId);
		//see comment in encodedLength
		buf.writeInt((int) body().size());
	}

	public static SaslMessage decode(ByteBuf buf) {
		if (buf.readByte() != TAG_BYTE) {
			throw new IllegalStateException("期望接收的是SaslMessage消息，" +
					"但是解析的消息不满足条件，可能是客户端没有开启SASL认证？");
		}
		String appId = CodecUtil.Strings.decode(buf);
		//see comment in encodedLength
		buf.readInt();
		return new SaslMessage(appId, buf.retain());
	}
}
