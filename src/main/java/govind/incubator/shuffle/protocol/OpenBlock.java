package govind.incubator.shuffle.protocol;

import com.google.common.base.Objects;
import govind.incubator.network.util.CodecUtil;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * 请求读取一系列blocks，返回值为StreamHandle。
 *
 */
public class OpenBlock extends BlockTransferMessage {

	public final String appId;
	public final String execId;
	public final String[] blockIds;

	public OpenBlock(String appId, String execId, String[] blockIds) {
		this.appId = appId;
		this.execId = execId;
		this.blockIds = blockIds;
	}

	@Override
	protected Type type() {
		return Type.OPEN_BLOCK;
	}

	@Override
	public int encodedLength() {
		return CodecUtil.Strings.encodedLength(appId) +
				CodecUtil.Strings.encodedLength(execId) +
				CodecUtil.StringArray.encodedLength(blockIds);
	}

	@Override
	public void encode(ByteBuf buf) {
		CodecUtil.Strings.encode(buf, appId);
		CodecUtil.Strings.encode(buf, execId);
		CodecUtil.StringArray.encode(buf, blockIds);
	}

	public static OpenBlock decode(ByteBuf buf) {
		String appId = CodecUtil.Strings.decode(buf);
		String execId = CodecUtil.Strings.decode(buf);
		String[] blockIds = CodecUtil.StringArray.decode(buf);
		return new OpenBlock(appId, execId, blockIds);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(appId, execId)*41 + Arrays.hashCode(blockIds);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof OpenBlock) {
			OpenBlock ob = (OpenBlock) obj;

			return Objects.equal(appId, ob.appId) &&
					Objects.equal(execId, ob.execId) &&
					Arrays.equals(blockIds, ob.blockIds);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("appId", appId)
				.add("execId", execId)
				.add("blockIds", Arrays.toString(blockIds))
				.toString();
	}
}
