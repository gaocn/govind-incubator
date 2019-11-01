package govind.incubator.network.sasl;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;

import javax.security.auth.callback.*;
import javax.security.sasl.*;
import java.io.IOException;
import java.util.Base64;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-30
 * <p>
 * A SASL Server for Govind which simply keeps track of the state of a
 * single SASL session, from the initial state to the "authenticated"
 * state. (It is not a server in the sense of accepting connections on
 * some socket.)
 */
@Slf4j
public class GovindSaslServer implements SaslEncryptionBackend {

	/**
	 * This is passed as the server name when creating the sasl
	 * client/server. This could be changed to be configurable
	 * in the future.
	 */
	public static final String DEFAULT_REALM = "default";

	/**
	 * The authentication mechanism used here is DIGEST-MD5. This
	 * could be changed to be configurable in the future.
	 *
	 * DIGEST-MD5 (RFC 2831)
	 * This mechanism defines how HTTP Digest Authentication can
	 * be used as a SASL mechanism.
	 *
	 * https://docs.oracle.com/javase/1.5.0/docs/guide/security/sasl/sasl-refguide.html
	 */
	public static final String DIGEST = "DIGEST-MD5";

	/**
	 * Quality of protection value that includes encryption.
	 */
	public static final String QOP_AUTH_CONF = "auth-conf";

	/**
	 * Quality of protection value that does not include encryption.
	 */
	public static final String QOP_AUTH = "auth";

	private final String secretKeyId;
	private final SecretKeyHolder secretKeyHolder;
	private SaslServer saslServer;

	public GovindSaslServer(
			String secretKeyId,
			SecretKeyHolder secretKeyHolder,
			boolean alwaysEncrypt) {
		this.secretKeyId = secretKeyId;
		this.secretKeyHolder = secretKeyHolder;

		//Sasl.QOP is  a comma-separated list of supported values. The value
		// that allows encryption is listed first since it's preferred over the
		// non-encrypted one (if the client also lists both in the request).
		String qop = alwaysEncrypt ?
				QOP_AUTH_CONF
				:
				String.format("%s,%s", QOP_AUTH_CONF, QOP_AUTH);
		ImmutableMap<String, String> saslProps = ImmutableMap.<String, String>builder()
				.put(Sasl.SERVER_AUTH, "true")
				.put(Sasl.QOP, qop)
				.build();

		try {
			this.saslServer = Sasl.createSaslServer(
					DIGEST,
					null,
					DEFAULT_REALM,
					saslProps,
					new DigestCallbackHandler());
		} catch (SaslException e) {
			throw Throwables.propagate(e);
		}

	}

	/**
	 * 判断认证过程是否成功
	 */
	public synchronized boolean isComplete() {
		return saslServer != null && saslServer.isComplete();
	}

	/**
	 * 获取协商的某个属性值
	 */
	public Object getNegotiatedProperty(String name) {
		return saslServer.getNegotiatedProperty(name);
	}

	/**
	 *  used to respond to server SASL tokens
	 * @param token Server's SASL Token
	 * @return response to send back to the server
	 */
	public synchronized byte[] response(byte[] token) {
		try {
			//Evaluates the response data and generates a challenge.
			return saslServer != null ? saslServer.evaluateResponse(token):new byte[0];
		} catch (SaslException e) {
			throw Throwables.propagate(e);
		}
	}

	/**
	 * Disposes of any system resources or security-sensitive information
	 * the SaslServer might be using
	 */
	@Override
	public void dispose() {
		if (saslServer != null) {
			try {
				saslServer.dispose();
			} catch (SaslException e) {
				//ignore
			} finally {
				saslServer = null;
			}
		}
	}

	@Override
	public byte[] wrap(byte[] data, int offset, int len) throws SaslException {
		return saslServer.wrap(data, offset, len);
	}

	@Override
	public byte[] unwrap(byte[] data, int offset, int len) throws SaslException {
		return saslServer.unwrap(data, offset, len);
	}

	/**
	 * Implementation of javax.security.auth.callback.CallbackHandler for
	 * SASL DIGEST-MD5 mechanism.
	 */
	private class DigestCallbackHandler implements CallbackHandler {

		@Override
		public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
			for (Callback callback : callbacks) {
				if (callback instanceof NameCallback) {
					log.info("SASL server callback: setting username");
					NameCallback nc = (NameCallback) callback;
					nc.setName(encodeIdentifier(secretKeyHolder.getSaslUser(secretKeyId)));
				} else if (callback instanceof PasswordCallback) {
					log.info("SASL server callback: setting password");
					PasswordCallback pc = (PasswordCallback) callback;
					pc.setPassword(encodePassword(secretKeyHolder.getSecretKey(secretKeyId)));
				} else if (callback instanceof RealmCallback) {
					log.info("SASL server callback: setting realm");
					RealmCallback rc = (RealmCallback) callback;
					rc.setText(rc.getDefaultText());
				} else if (callback instanceof AuthorizeCallback) {
					AuthorizeCallback ac = (AuthorizeCallback) callback;
					//认证，验证是否是正确的用户名和密码
					String authId = ac.getAuthenticationID();
					//授权，验证是否有某个操作的权限
					String authzId = ac.getAuthorizationID();
					ac.setAuthorized(authId.equals(authzId));
					if (ac.isAuthorized()) {
						ac.setAuthorizedID(authzId);
					}
					log.info("SASL Authorization Complete, authorized set to {}", ac.isAuthorized());
				} else {
					throw new UnsupportedCallbackException(callback, "SASL DIGEST-MD5认证中无法识别并处理的回调");
				}
			}
		}
	}

	/**
	 * 将用户名用BASE64进行加密
	 *
	 * @param identifier
	 * @return
	 */
	public static String encodeIdentifier(String identifier) {
		Preconditions.checkNotNull(identifier, "SASL安全认证开启时，用户名不能为空");
		return Base64.getEncoder().encodeToString(identifier.getBytes(Charsets.UTF_8));
	}

	/**
	 * 将密码用BASE64进行加密
	 *
	 * @param password
	 * @return
	 */
	public static char[] encodePassword(String password) {
		Preconditions.checkNotNull(password, "SASL安全认证开启时，密码不能为空");
		return Base64.getEncoder().encodeToString(password.getBytes(Charsets.UTF_8)).toCharArray();
	}
}
