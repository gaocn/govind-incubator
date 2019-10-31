package govind.incubator.network.sasl;

import govind.incubator.network.client.TransportClient;
import govind.incubator.network.client.TransportClientBootstrap;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.util.NettyUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-31
 *
 * Bootstraps a {@link TransportClient} by performing SASL authentication
 * on the connection. The server should be setup with a {@link SaslRpcHandler}
 * with matching keys for the given appId.
 *
 */
@Slf4j
public class SaslClientBootstrap implements TransportClientBootstrap {
	private final boolean encrypt;
	private final String appId;
	private final TransportConf conf;
	private final SecretKeyHolder secretKeyHolder;

	public SaslClientBootstrap(String appId, TransportConf conf, SecretKeyHolder secretKeyHolder) {
		this(false, appId, conf, secretKeyHolder);
	}

	public SaslClientBootstrap(boolean encrypt, String appId, TransportConf conf, SecretKeyHolder secretKeyHolder) {
		this.encrypt = encrypt;
		this.appId = appId;
		this.conf = conf;
		this.secretKeyHolder = secretKeyHolder;
	}

	/**
	 * Performs SASL authentication by sending a token, and then proceeding
	 * with the SASL  challenge-response tokens until we either successfully
	 * authenticate or throw an exception due to mismatch.
	 *
	 * @param client
	 * @param channel
	 * @throws RuntimeException
	 */
	@Override
	public void doBootstrap(TransportClient client, Channel channel) throws RuntimeException {
		GovindSaslClient saslClient = new GovindSaslClient(secretKeyHolder, appId, encrypt);

		try {
			byte[] token = saslClient.firstToken();

			while (!saslClient.isComplete()) {
				SaslMessage saslMessage = new SaslMessage(appId, token);
				ByteBuf buf = Unpooled.buffer(saslMessage.encodedLength() + (int) saslMessage.body().size());
				saslMessage.encode(buf);
				buf.writeBytes(saslMessage.body().nioByteBuffer());

				ByteBuffer response = client.sendRpcSync(buf.nioBuffer(), conf.saslTimeoutMS());
				token = saslClient.response(NettyUtil.bufToArray(response));
			}

			client.setClientId(appId);
			if (encrypt) {
				if (!GovindSaslServer.QOP_AUTH_CONF.equals(saslClient.getNegotiatedProperty(Sasl.QOP))) {
					throw new RuntimeException(
							new SaslException("Encryption requests by negotiated non-encrypted connection.")
					);
				}
				SaslEncryption.addToChannel(channel, saslClient, conf.maxSaslEncryptedBlockSize());
				saslClient = null;
				log.info("通道{}配置为SASL加密方式通信", channel);
			}
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		} finally {
			if (saslClient != null) {
				try {
					//Once authentication is complete, the server will trust all remaining communication.
					saslClient.dispose();
				} catch (Exception e) {
					log.info("释放SASL Client资源时出错：{}。", e.getMessage());
				}
			}
		}
	}
}
