package govind.incubator.shuffle.protocol;

import com.google.common.base.Objects;
import govind.incubator.network.util.CodecUtil;
import io.netty.buffer.ByteBuf;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * Driver发送给MesosExternalShuffleService的注册请求
 *
 */
public class RegisterDriver extends BlockTransferMessage {
	private final String appId;

	public RegisterDriver(String appId) {
		this.appId = appId;
	}

	@Override
	protected Type type() {
		return Type.REGISTER_DRIVER;
	}

	@Override
	public int encodedLength() {
		return CodecUtil.Strings.encodedLength(appId);
	}

	@Override
	public void encode(ByteBuf buf) {
		CodecUtil.Strings.encode(buf, appId);
	}

	public static RegisterDriver decode(ByteBuf buf) {
		String appId = CodecUtil.Strings.decode(buf);
		return new RegisterDriver(appId);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(appId);
	}

	@Override
	public boolean equals(Object obj) {
		if ( obj!= null && obj instanceof RegisterDriver) {
			return Objects.equal(appId, ((RegisterDriver) obj).appId);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("appId",  appId)
				.toString();
	}
}
