package govind.incubator.network.protocol;

import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;
import jersey.repackaged.com.google.common.base.MoreObjects;

/**
 * Rpc请求
 */
public class RpcRequest extends AbstractMessage implements RequestMessage {
	/**
	 * 请求标识，RpcRequest和RpcResponse响应报文的标识相同！
	 */
	final long requestId;

	public RpcRequest(long requestId,  ManagedBuffer body) {
		super(body, true);
		this.requestId = requestId;
	}

	@Override
	public Type type() {
		return Type.RpcRequest;
	}

	@Override
	public int encodedLength() {
		return 8 + 4;
	}

	@Override
	public void encode(ByteBuf buf) {
		buf.writeLong(requestId);
		buf.writeInt((int)body().size());
	}

	public static RpcRequest decode(ByteBuf buf) {
		long requestId = buf.readLong();
		buf.readInt();
		return new RpcRequest(requestId, new NettyManagedBuffer(buf.retain()));
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RpcRequest) {
			RpcRequest req = (RpcRequest)obj;
			return this.requestId  == req.requestId && super.equals(req);
		}
		return false;
	}

	@Override
	public String toString() {
		 return MoreObjects.toStringHelper(this)
				 .add("requestId", requestId)
				 .add("body", body())
				 .toString();
	}
}
