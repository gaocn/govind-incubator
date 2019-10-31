package govind.incubator.network.sasl;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;

import javax.security.auth.callback.*;
import javax.security.sasl.*;
import java.io.IOException;

import static govind.incubator.network.sasl.GovindSaslServer.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-30
 *
 * A SASL Client simply keeps track of the state of a single SASL session,
 * from the initial state to the "authenticated" state. This client initializes
 * the protocol via a firstToken, which is then followed by a set of challenges
 * and responses.
 */
@Slf4j
public class GovindSaslClient implements SaslEncryptionBackend {

	private final SecretKeyHolder secretKeyHolder;
	private final String secretKeyId;
	private final String expectedQop;
	private SaslClient saslClient;

	public GovindSaslClient(SecretKeyHolder secretKeyHolder, String secretKeyId, boolean encrypt) {
		this.secretKeyHolder = secretKeyHolder;
		this.secretKeyId = secretKeyId;
		this.expectedQop = encrypt ? QOP_AUTH_CONF : QOP_AUTH;

		ImmutableMap<String, String> saslProps = ImmutableMap.<String, String>builder()
				.put(Sasl.QOP, expectedQop)
				.build();

		try {

			this.saslClient = Sasl.createSaslClient(
					new String[]{DIGEST},
					null,
					null,
					DEFAULT_REALM,
					saslProps,
					new ClientCallbackHandler());

		} catch (SaslException e) {
			throw Throwables.propagate(e);
		}
	}


	/**
	 * 用于生成首次握手时的挑战信息
	 */
	public synchronized byte[] firstToken() {
		if (saslClient != null && saslClient.hasInitialResponse()) {
			try {
				return saslClient.evaluateChallenge(new byte[0]);
			} catch (SaslException e) {
				throw Throwables.propagate(e);
			}
		} else {
			return new byte[0];
		}
	}

	/**
	 * 判断认证过程是否成功
	 * @return
	 */
	public synchronized boolean isComplete() {
		return saslClient != null && saslClient.isComplete();
	}

	/**
	 * Respond to server's SASL token.
	 * @param token contains server's SASL token
	 * @return client's response SASL token
	 */
	public synchronized byte[] response(byte[] token) {
		try {
			return saslClient != null?saslClient.evaluateChallenge(token):new byte[0];
		} catch (SaslException e) {
			throw Throwables.propagate(e);
		}
	}

	/**
	 * 获取协商的某个属性值
	 */
	public Object getNegotiatedProperty(String name) {
		return saslClient.getNegotiatedProperty(name);
	}

	/**
	 * Disposes of any system resources or security-sensitive information
	 * the SaslClient might be using.
	 */
	@Override
	public void dispose() {
		if (saslClient != null) {
			try {
				saslClient.dispose();
			} catch (SaslException e) {
				//ignore
			} finally {
				saslClient = null;
			}
		}
	}

	@Override
	public byte[] wrap(byte[] data, int offset, int len) throws SaslException {
		return saslClient.wrap(data, offset, len);
	}

	@Override
	public byte[] unwrap(byte[] data, int offset, int len) throws SaslException {
		return saslClient.unwrap(data, offset, len);
	}

	/**
	 * Implementation of javax.security.auth.callback.CallbackHandler
	 * that works with share secrets.
	 */
	private class ClientCallbackHandler implements CallbackHandler {
		@Override
		public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
			for (Callback callback : callbacks) {
				if (callback instanceof NameCallback) {
					NameCallback nc = (NameCallback) callback;
					log.info("SASL client callback: setting username");
					nc.setName(encodeIdentifier(secretKeyHolder.getSaslUser(secretKeyId)));
				} else if (callback instanceof PasswordCallback) {
					PasswordCallback pc = (PasswordCallback) callback;
					log.info("SASL client callback: setting password");
					pc.setPassword(encodePassword(secretKeyHolder.getSecretKey(secretKeyId)));
				} else if (callback instanceof RealmCallback) {
					RealmCallback rc = (RealmCallback) callback;
					log.info("SASL client callback: setting realm");
					rc.setText(rc.getDefaultText());
				} else if (callback instanceof RealmChoiceCallback) {
					//ignore
				} else {
					throw new UnsupportedCallbackException(callback, "SASL DIGEST-MD5认证中无法识别并处理的回调");
				}
			}
		}
	}
}
