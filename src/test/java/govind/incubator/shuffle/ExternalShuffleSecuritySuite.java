package govind.incubator.shuffle;

import com.google.common.collect.Lists;
import govind.incubator.network.conf.SystemPropertyConfigProvider;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.sasl.SaslServerBootstrap;
import govind.incubator.network.sasl.SecretKeyHolder;
import govind.incubator.network.server.TransportServer;
import govind.incubator.network.util.NettyUtil;
import govind.incubator.network.util.TransportContext;
import govind.incubator.shuffle.protocol.ExecutorShuffleInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-5
 */
public class ExternalShuffleSecuritySuite {
	TransportConf conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");
	TransportServer server;

	@Before
	public void beforeEach() throws IOException {
		TransportContext context = new TransportContext(conf, new ExternalShuffleBlockHandler(conf, null));

		SaslServerBootstrap bootstrap = new SaslServerBootstrap(conf, new TestSecretKeyHolder("my-app-id", "secret"));

		server = context.createServer(Arrays.asList(bootstrap));
	}

	@After
	public void afterEach(){
		if (server != null) {
			server.close();
			server = null;
		}
	}

	@Test
	public void testValid() throws IOException {
		validate("my-app-id", "secret", false);
	}

	@Test
	public void testBadAppId() {
		try {
			validate("wrong-app-id", "secret", false);
		} catch (Exception e) {
			assertTrue(e.getMessage(), e.getMessage().contains("appId不匹配"));
		}
	}

	@Test
	public void testBadSecret() {
		try {
			validate("my-app-id", "wrong-secret", false);
		} catch (Exception e) {
			assertTrue(e.getMessage(), e.getMessage().contains("Mismatched response"));
		}
	}

	@Test
	public void testEncryption() throws IOException {
		validate("my-app-id", "secret", true);
	}


	private void validate(String appId, String secretKey, boolean encrypt) throws IOException {
		ExternalShuffleClient client = new ExternalShuffleClient(conf, new TestSecretKeyHolder(appId, secretKey), true, encrypt);

		client.init(appId);
		client.registerWithShuffleServer(NettyUtil.getLocalHost(), server.getPort(), "exec0", new ExecutorShuffleInfo(new String[0], 0, ""));
		client.close();
	}

	public static class TestSecretKeyHolder implements SecretKeyHolder {
		final String appId;
		final String secretKey;

		public TestSecretKeyHolder(String appId, String secretKey) {
			this.appId = appId;
			this.secretKey = secretKey;
		}

		@Override
		public String getSaslUser(String appId) {
			return "user";
		}

		@Override
		public String getSecretKey(String appId) {
			if (!appId.equals(this.appId)) {
				throw new IllegalArgumentException("appId不匹配");
			}
			return secretKey;
		}
	}
}
