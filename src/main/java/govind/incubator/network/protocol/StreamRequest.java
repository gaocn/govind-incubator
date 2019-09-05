package govind.incubator.network.protocol;

import com.google.common.base.Objects;
import govind.incubator.network.util.CodecUtil;
import govind.incubator.network.util.CodecUtil.Strings;
import io.netty.buffer.ByteBuf;

/**
 * 向服务端请求stream data
 *
 * 在数据流动前，streamId为任意字符串并且需要端与端进行协商后确定!
 */
public class StreamRequest extends AbstractMessage implements RequestMessage{
	public final String  streamId;

	public StreamRequest(String streamId) {
		this.streamId = streamId;
	}

	@Override
	public Type type() {
		return Type.StreamRequest;
	}

	@Override
	public int encodedLength() {
		return CodecUtil.Strings.encodedLength(streamId);
	}

	@Override
	public void encode(ByteBuf buf) {
		CodecUtil.Strings.encode(buf, streamId);
	}

	public static StreamRequest decode(ByteBuf buf) {
		String streamId = Strings.decode(buf);
		return new StreamRequest(streamId);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(streamId);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StreamRequest) {
			StreamRequest o = (StreamRequest) obj;
			return streamId.equals(o.streamId);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("streamId", streamId)
				.toString();
	}
}
