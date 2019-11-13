package govind.incubator.launcher;

import govind.incubator.launcher.GovindAppHandle.State;

import java.io.Serializable;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-8
 *
 * Message definitions for the launcher communication protocol. These messages
 * must remain backwards-compatible, so that the launcher can talk to older
 * versions of Spark that support the protocol.
 *
 */
final public class LauncherProtocol {
	/** Environment variable where the server port is stored. */
		static final String ENV_LAUNCHER_PORT = "_SPARK_LAUNCHER_PORT";
	/** Environment variable where the secret for connecting back to the server is stored. */
	static final String ENV_LAUNCHER_SECRET = "_SPARK_LAUNCHER_SECRET";


	/**
	 *	Message Definition
	 */
	static class Message implements Serializable{}

	/** Hello message, sent from client to server. */
	static class Hello extends Message{
		final String secret;
		final String version;

		public Hello(String secret, String version) {
			this.secret = secret;
			this.version = version;
		}
	}

	/** SetAppId message, sent from client to server. */
	static class SetAppId extends Message{
		final String appId;

		public SetAppId(String appId) {
			this.appId = appId;
		}
	}

	/** SetState message, sent from client to server. */
	static class SetState extends Message {
		final GovindAppHandle.State state;

		public SetState(State state) {
			this.state = state;
		}
	}

	/** Stop message, send from server to client to stop the application. */
	static class StopMessage extends Message{}
}
