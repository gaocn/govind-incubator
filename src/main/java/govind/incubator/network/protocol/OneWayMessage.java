package govind.incubator.network.protocol;

import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;
import jersey.repackaged.com.google.common.base.MoreObjects;

/**
 * 请求
 */
public class OneWayMessage extends AbstractMessage implements RequestMessage {

	public OneWayMessage(ManagedBuffer body) {
		super(body, true);
	}

	@Override
	public Type type() {
		return Type.OneWayMessage;
	}

	public static OneWayMessage decode(ByteBuf buf) {
		buf.readInt();
		return new OneWayMessage(new NettyManagedBuffer(buf.retain()));
	}

	@Override
	public int encodedLength() {
		return 4;
	}

	@Override
	public void encode(ByteBuf buf) {
		buf.writeInt((int)body().size());
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof OneWayMessage) {
			return super.equals((OneWayMessage)obj);
		}
		return false;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("body", body())
				.toString();
	}
}
