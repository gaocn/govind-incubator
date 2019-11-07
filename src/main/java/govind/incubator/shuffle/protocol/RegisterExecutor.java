package govind.incubator.shuffle.protocol;

import com.google.common.base.Objects;
import govind.incubator.network.util.CodecUtil;
import io.netty.buffer.ByteBuf;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * Executor先Shuffle Server注册时发送的请求，响应为空(empty byte array)
 *
 */
public class RegisterExecutor extends BlockTransferMessage {
	public final String appId;
	public final String execId;
	public final ExecutorShuffleInfo executorShuffleInfo;


	public RegisterExecutor(String appId, String execId, ExecutorShuffleInfo executorShuffleInfo) {
		this.appId = appId;
		this.execId = execId;
		this.executorShuffleInfo = executorShuffleInfo;
	}

	@Override
	protected Type type() {
		return Type.REGISTER_EXECUTOR;
	}

	@Override
	public int encodedLength() {
		return CodecUtil.Strings.encodedLength(appId)
				+ CodecUtil.Strings.encodedLength(execId)
				+ executorShuffleInfo.encodedLength();
	}

	@Override
	public void encode(ByteBuf buf) {
		CodecUtil.Strings.encode(buf, appId);
		CodecUtil.Strings.encode(buf, execId);
		executorShuffleInfo.encode(buf);
	}

	public static RegisterExecutor decode(ByteBuf buf) {
		String appId = CodecUtil.Strings.decode(buf);
		String execId = CodecUtil.Strings.decode(buf);
		ExecutorShuffleInfo executorShuffleInfo = ExecutorShuffleInfo.decode(buf);
		return new RegisterExecutor(appId, execId, executorShuffleInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(appId, execId) * 41 + executorShuffleInfo.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof RegisterExecutor) {
			RegisterExecutor re = (RegisterExecutor) obj;
			return Objects.equal(appId, re.appId)
					&& Objects.equal(execId, re.execId)
					&& executorShuffleInfo.equals(re.executorShuffleInfo);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("appId", appId)
				.add("execId", execId)
				.add("executorShuffleInfo", executorShuffleInfo)
				.toString();
	}
}
