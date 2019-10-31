package govind.incubator.network.sasl;

import govind.incubator.network.client.TransportClient;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.network.handler.StreamManager;
import govind.incubator.network.util.NettyUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import javax.security.sasl.Sasl;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-30
 *
 * RPC Handler which performs SASL authentication before delegating to a
 * child RPC handler. The delegate will only receive messages if the given
 * connection has been successfully authenticated. A connection may be
 * authenticated at most once.
 *
 * Note that the authentication process consists of multiple challenge-response
 * pairs, each ofwhich are individual RPCs.
 *
 */
@Slf4j
public class SaslRpcHandler extends RpcHandler {
	/** 传输层配置 */
	private final TransportConf conf;
	/** 客户端通道 */
	private final Channel channel;
	/** 当前实例负责认证，消息处理代理给RpcHandler */
	private final RpcHandler delegate;
	/** 存储用户名和密码的实例，由GovindSaslClient和GovindSaslServer共享，且是per-app basis */
	private final SecretKeyHolder secretKeyHolder;

	private GovindSaslServer saslServer;
	private boolean isComplete;

	public SaslRpcHandler(
			TransportConf conf,
			Channel channel,
			RpcHandler delegate,
			SecretKeyHolder secretKeyHolder) {
		this.conf = conf;
		this.channel = channel;
		this.delegate = delegate;
		this.secretKeyHolder = secretKeyHolder;
		this.saslServer = null;
		this.isComplete = false;
	}

	@Override
	public StreamManager getStreamManager() {
		return delegate.getStreamManager();
	}

	@Override
	public void connectionTerminated(TransportClient client) {
		try {
			delegate.connectionTerminated(client);
		} finally {
			if (saslServer != null) {
				saslServer.dispose();
			}
		}
	}

	@Override
	public void exceptionCaught(Throwable cause, TransportClient client) {
		delegate.exceptionCaught(cause, client);
	}

	@Override
	public void receive(TransportClient client, ByteBuffer msg) {
		delegate.receive(client, msg);
	}

	@Override
	public void receive(TransportClient client, ByteBuffer msg, RpcCallback callback) {
		if (isComplete) {
			delegate.receive(client, msg, callback);
			return;
		}

		ByteBuf nettyBuf = Unpooled.wrappedBuffer(msg);
		SaslMessage saslMessage = null;
		try {
			saslMessage = SaslMessage.decode(nettyBuf);
		} catch (Exception e) {
			nettyBuf.release();
		}

		if (saslServer == null) {
			//first message in handshake, setup necessary state
			client.setClientId(saslMessage.appId);
			saslServer = new GovindSaslServer(saslMessage.appId, secretKeyHolder, conf.saslServerAlwaysEncrypt());
		}

		byte[] response = null;
		try {
			response = saslServer.response(NettyUtil.bufToArray(saslMessage.body().nioByteBuffer()));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		callback.onSuccess(ByteBuffer.wrap(response));

		//Setup encryption after the SASL response is sent, otherwise the client
		// can't parse the response. It's ok to change the channel pipeline here
		// since we are processing an incoming essage, so the pipeline is busy and
		// no new incoming messages will be fed to it before this method returns.
		// This assumes that the code ensures, through other means, that no outbound
		// messages are being written to the channel while negotiation is still going on.
		if (saslServer.isComplete()) {
			log.info("通道{}SASL认证成功", channel);
			isComplete = true;
			if (GovindSaslServer.QOP_AUTH_CONF.equals(saslServer.getNegotiatedProperty(Sasl.QOP))) {
				log.info("为通道{}开启加密功能", client);
				SaslEncryption.addToChannel(channel, saslServer, conf.maxSaslEncryptedBlockSize());
				saslServer = null;
			} else {
				saslServer.dispose();
				saslServer = null;
			}
		}

	}
}
