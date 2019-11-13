package govind.incubator.launcher;

import govind.incubator.launcher.LauncherProtocol.StopMessage;
import govind.incubator.launcher.util.NamedThreadFactory;
import govind.incubator.launcher.util.OutputRedirector;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

import static govind.incubator.launcher.util.CommandBuilderUtils.checkState;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-11
 *
 * Handle implementation for monitoring apps started as a child process
 *
 */
@Slf4j
public class ChildProcAppHandle implements GovindAppHandle{
	/** 用于处理子进程输出流的线程工厂 */
	static final ThreadFactory REDIRECT_FACTORY = new NamedThreadFactory("launcher-proc-%d");

	final String secrect;
	final LauncherServer server;

	private Process childProc;
	private boolean disposed;
	private LauncherConnection connection;
	private List<Listener> listeners;
	private State state;
	private String appId;
	private OutputRedirector redirector;

	public ChildProcAppHandle(String secrect, LauncherServer server) {
		this.secrect = secrect;
		this.server = server;
		this.state = State.UNKNOWN;
	}

	@Override
	public synchronized void addListener(Listener listener) {
		if (this.listeners == null) {
			this.listeners = new ArrayList<>();
		}
		listeners.add(listener);
	}

	@Override
	public State getState() {
		return state;
	}

	@Override
	public String getAppId() {
		return appId;
	}

	@Override
	public void stop() {
		checkState(connection != null,  "app尚未连接到LauncherServer");
		try {
			connection.send(new StopMessage());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void kill() {
		if (!disposed) {
			disconnect();
		}

		if (childProc != null) {
			try {
				childProc.exitValue();
			} catch (IllegalThreadStateException e) {
				// Child is still alive. Try to use Java 8's "destroyForcibly()"
				// if available, fall back to the old API if it's not there.
				try {
					Method destory = childProc.getClass().getMethod("destroyForcibly");
					destory.invoke(childProc);
				} catch (Exception inner) {
					childProc.destroy();
				}
			} finally {
				childProc = null;
			}
		}
	}

	@Override
	public synchronized void disconnect() {
		if (!disposed) {
			disposed = true;
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					//NOP
				}
			}
			server.unregister(this);
			if (redirector != null) {
				redirector.stop();
			}
		}
	}

	public String getSecrect() {
		return secrect;
	}

	public void setChildProc(Process childProc, String loggerName) {
		this.childProc = childProc;
		this.redirector = new OutputRedirector(childProc.getInputStream(), loggerName, REDIRECT_FACTORY);
	}

	public void setConnection(LauncherConnection connection) {
		this.connection = connection;
	}

	public LauncherServer getServer() {
		return server;
	}

	public LauncherConnection getConnection() {
		return connection;
	}

	public void setAppId(String appId) {
		this.appId = appId;
		fireEvent(true);
	}

	public void setState(State s) {
		if (!state.isFinal()) {
			this.state = s;
			fireEvent(false);
		} else {
			log.warn("Backend requested transition from final state {} to {}", state, s);
		}
	}

	private synchronized void fireEvent(boolean isInfoChanged) {
		if (listeners != null) {
			for (Listener l : listeners) {
				if (isInfoChanged) {
					l.infoChanged(this);
				} else {
					l.stateChanged(this);
				}
			}
		}
	}
}
