package govind.incubator.launcher;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-7
 *
 * A handle to a running Spark application.
 *
 * Provides runtime information about the underlying Spark application,
 * and actions to control it.
 *
 */
public interface GovindAppHandle {
	/**
	 * Represents the application's state. A state can be "final", in
	 * which case it will not change after it's reached, and means the
	 * application is not running anymore.
	 */
	enum State {
		/** The application has not reported back yet. */
		UNKNOWN(false),
		/** The application has connected to the handle. */
		CONNECTED(false),
		/** The application has been submitted to the cluster.  */
		SUBMITTED(false),
		/** The application is running. */
		RUNNING(false),
		/** The application finished with a successful status. */
		FINISHED(true),
		/** The application finished with a failed status. */
		FAILED(true),
		/**  The application was killed. */
		KILLED(true);

		private final boolean isFinal;

		State(boolean isFinal) {
			this.isFinal = isFinal;
		}

		public boolean isFinal() {
			return isFinal;
		}
	}

	/**
	 * Listener for updates to a handle's state. The callbacks do not
	 * receive information about what exactly has changed, just that
	 * an update has occurred.
	 */
	interface Listener {
		/**
		 * Callback for changes in the handle's state.
		 * @param handle
		 */
		void stateChanged(GovindAppHandle handle);

		/**
		 *  Callback for changes in any information that is not the handle's state.
		 * @param handle
		 */
		void infoChanged(GovindAppHandle handle);
	}

	/**
	 * Adds a listener to be notified of changes to the handle's information.
	 * Listeners will be called  from the thread processing updates from the
	 * application, so they should avoid blocking or long-running operations.
	 *
	 * @param listener Listener to add.
	 */
	void addListener(Listener listener);

	/** Returns the current application state */
	State getState();

	/** Returns the application ID, or <code>null</code> if not yet known.*/
	String getAppId();

	/**
	 * Asks the application to stop. This is best-effort, since the application
	 * may fail to receive or act on the command. Callers should watch for a state
	 * transition that indicates the  application has really stopped.
	 */
	void stop();

	/**
	 * Tries to kill the underlying application. Implies {@link #disconnect()}.
	 * This will not send a {@link #stop()} message to the application, so it's
	 * recommended that users first try to stop the application cleanly and only
	 * resort to this method if that fails.
	 *
	 * Note that if the application is running as a child process, this method
	 * fail to kill the process when using Java 7. This may happen if, for example,
	 * the application is deadlocked.
	 */
	void kill();

	/**
	 *  Disconnects the handle from the application, without stopping it. After this
	 *  method is called, the handle will not be able to communicate with the
	 *  application anymore.
	 *
	 */
	void disconnect();
}
