package govind.incubator.network.protocol;

import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;
import jersey.repackaged.com.google.common.base.MoreObjects;

public class RpcResponse extends AbstractResponseMessage{
	public final long requestId;

	public RpcResponse(ManagedBuffer body, long requestId) {
		super(body, true);
		this.requestId = requestId;
	}

	@Override
	public ResponseMessage createFailureResponse(String msg) {
		return new RpcFailure(requestId, msg);
	}

	@Override
	public Type type() {
		return Type.RpcResponse;
	}

	@Override
	public int encodedLength() {
		return 8+4;
	}

	@Override
	public void encode(ByteBuf buf) {
		buf.writeLong(requestId);
		buf.writeInt((int)body().size());
	}

	public static RpcResponse decode(ByteBuf buf) {
		long requestId = buf.readLong();
		buf.readInt();
		return new RpcResponse(new NettyManagedBuffer(buf.retain()), requestId);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RpcResponse) {
			RpcResponse o = (RpcResponse)obj;
			return this.requestId == o.requestId &&
					super.equals(o);
		}
		return false;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("reuestId", requestId)
				.add("body", body())
				.toString();
	}
}
