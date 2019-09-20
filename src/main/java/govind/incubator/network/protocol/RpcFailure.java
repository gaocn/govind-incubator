package govind.incubator.network.protocol;

import govind.incubator.network.util.CodecUtil;
import io.netty.buffer.ByteBuf;

import java.util.Objects;

public class RpcFailure extends AbstractMessage implements ResponseMessage {

	final long requestId;
	final String error;

	public RpcFailure(long requestId, String errorMsg) {
		this.requestId = requestId;
		this.error = errorMsg;
	}

	@Override
	public Type type() {
		return Type.RpcFailure;
	}

	@Override
	public int encodedLength() {
		return 8 + CodecUtil.Strings.encodedLength(error);
	}

	@Override
	public void encode(ByteBuf buf) {
		buf.writeLong(requestId);
		CodecUtil.Strings.encode(buf, error);
	}

	public static RpcFailure decode(ByteBuf buf) {
		long requestId = buf.readLong();
		String error =  CodecUtil.Strings.decode(buf);
		return new RpcFailure(requestId, error);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(this);
	}

	@Override
	public String toString() {
		return Objects.toString(this);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof RpcFailure) {
			RpcFailure rpcFailure = (RpcFailure)o;
			return this.requestId == rpcFailure.requestId &&
					this.error.equals(rpcFailure.error);
		}

		return false;
	}
}
