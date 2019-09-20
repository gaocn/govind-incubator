package govind.incubator.network.protocol;

import com.google.common.base.Objects;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.util.CodecUtil;
import govind.incubator.network.util.CodecUtil.Strings;
import io.netty.buffer.ByteBuf;

public class StreamResponse extends AbstractResponseMessage {
	final String streamId;
	final long byteCount;

	public StreamResponse(ManagedBuffer body, String streamId, long byteCount) {
		super(body, true);
		this.streamId = streamId;
		this.byteCount = byteCount;
	}

	@Override
	public ResponseMessage createFailureResponse(String error) {
		return new StreamFailure(streamId, error);
	}

	@Override
	public Type type() {
		return Type.StreamResponse;
	}

	@Override
	public int encodedLength() {
		return 8 + CodecUtil.Strings.encodedLength(streamId);
	}

	@Override
	public void encode(ByteBuf buf) {
		CodecUtil.Strings.encode(buf, streamId);
		buf.writeLong(byteCount);
	}

	public static StreamResponse decode(ByteBuf buf) {
		String streamId = Strings.decode(buf);
		long byteCount = buf.readLong();
		//因为Stream是及时生效，在反序列化时Stream已经不存在了，因此为空！
		return new StreamResponse(null, streamId, byteCount);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(streamId, byteCount);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StreamResponse) {
			StreamResponse o = (StreamResponse) obj;
			return streamId.equals(o.streamId)  &&
					byteCount == o.byteCount;
		}

		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("streamId", streamId)
				.add("byteCount", byteCount)
				.toString();
	}
}
