package govind.incubator.shuffle.protocol;


import com.google.common.base.Objects;
import govind.incubator.network.util.CodecUtil;
import govind.incubator.network.util.CodecUtil.Strings;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * 请求上传某个block数据，并设定StorageLevel，返回空(empty byte array)
 *
 */
public class UploadBlock extends BlockTransferMessage {
	public final String appId;
	public final String execId;
	public final String blockId;

	//TODO: StorageLevel存储级别与数据是分开序列化的，因为StorageLevel当前package不可用！
	public final byte[] metadata;
	public final byte[] blockData;

	/**
	 * @param metadata 元数据，例如存储级别信息；
	 * @param blockData 实际数据
	 */
	public UploadBlock(
			String appId,
			String execId,
			String blockId,
			byte[] metadata,
			byte[] blockData) {
		this.appId = appId;
		this.execId = execId;
		this.blockId = blockId;
		this.metadata = metadata;
		this.blockData = blockData;
	}

	@Override
	protected Type type() {
		return Type.UPLOAD_BLOCK;
	}

	@Override
	public int encodedLength() {
		return CodecUtil.Strings.encodedLength(appId) +
				CodecUtil.Strings.encodedLength(execId) +
				CodecUtil.Strings.encodedLength(blockId) +
				CodecUtil.ByteArray.encodedLength(metadata) +
				CodecUtil.ByteArray.encodedLength(blockData);
	}

	@Override
	public void encode(ByteBuf buf) {
		CodecUtil.Strings.encode(buf, appId);
		CodecUtil.Strings.encode(buf, execId);
		CodecUtil.Strings.encode(buf, blockId);
		CodecUtil.ByteArray.encode(buf, metadata);
		CodecUtil.ByteArray.encode(buf, blockData);
	}

	public static UploadBlock decode(ByteBuf buf) {
		String appId = Strings.decode(buf);
		String execId = Strings.decode(buf);
		String blockId = Strings.decode(buf);
		byte[] metadata = CodecUtil.ByteArray.decode(buf);
		byte[] blockData = CodecUtil.ByteArray.decode(buf);
		return new UploadBlock(appId, execId, blockId, metadata, blockData);
	}

	@Override
	public int hashCode() {
		return (Objects.hashCode(appId, execId, blockId) * 41 + Arrays.hashCode(metadata)) * 41  + Arrays.hashCode(blockData);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof UploadBlock) {
			UploadBlock ub = (UploadBlock) obj;
			return Objects.equal(appId, ub.appId) &&
					Objects.equal(execId, ub.execId) &&
					Objects.equal(blockId, ub.blockId) &&
					Arrays.equals(metadata, ub.metadata) &&
					Arrays.equals(blockData, ub.blockData);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("appId", appId)
				.add("execId", execId)
				.add("blockId", blockId)
				.toString();
	}
}
