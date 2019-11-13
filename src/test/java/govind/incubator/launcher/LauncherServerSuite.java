package govind.incubator.launcher;

import com.google.common.io.Closeables;
import govind.incubator.launcher.GovindAppHandle.Listener;
import govind.incubator.launcher.GovindAppHandle.State;
import govind.incubator.launcher.LauncherProtocol.*;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static govind.incubator.launcher.util.LauncherConsts.CHILD_CONNECTION_TIMEOUT;
import static junit.framework.TestCase.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-13
 */
public class LauncherServerSuite {

	@Test
	public void testLauncherServerReuse() throws IOException {
		ChildProcAppHandle handle1 = null;
		ChildProcAppHandle handle2 = null;
		ChildProcAppHandle handle3 = null;

		try {
			handle1 = LauncherServer.newAppHandler();
			handle2 = LauncherServer.newAppHandler();
			LauncherServer server1 = LauncherServer.getServerInstance();
			assertSame(server1, handle1.getServer());

			handle1.kill();
			handle2.kill();

			handle3 = LauncherServer.newAppHandler();
			assertNotSame(server1, handle3.getServer());

			handle3.kill();
			assertNull(LauncherServer.getServerInstance());
		} finally {
			kill(handle1);
			kill(handle2);
			kill(handle3);
		}
	}

	@Test
	public void testCommunication() throws Exception {
		ChildProcAppHandle handle = LauncherServer.newAppHandler();
		TestClient client = null;

		try {
			final Object waitLock = new Object();
			Socket socket = new Socket(InetAddress.getLoopbackAddress(), LauncherServer.getServerInstance().getPort());

			handle.addListener(new Listener() {
				@Override
				public void stateChanged(GovindAppHandle handle) {
					wakeup();
				}

				@Override
				public void infoChanged(GovindAppHandle handle) {
					wakeup();
				}

				private void wakeup() {
					synchronized (waitLock) {
						waitLock.notifyAll();
					}
				}
			});
			client =  new TestClient(socket);

			synchronized (waitLock) {
				client.send(new Hello(handle.getSecrect(),"1.6.3"));
				waitLock.wait(TimeUnit.SECONDS.toMillis(10));
			}
			assertNotNull(handle.getConnection());

			synchronized (waitLock) {
				client.send(new SetAppId("app-id"));
				waitLock.wait(TimeUnit.SECONDS.toMillis(10));
			}
			assertEquals("app-id", handle.getAppId());

			synchronized (waitLock) {
				client.send(new SetState(State.RUNNING));
				waitLock.wait(TimeUnit.SECONDS.toMillis(10));
			}
			assertEquals(State.RUNNING, handle.getState());

			handle.stop();
			Message stopMsg = client.inbound.poll(10, TimeUnit.SECONDS);
			assertTrue(stopMsg instanceof StopMessage);
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			kill(handle);
			Closeables.closeQuietly(client);
			client.clientThread.join();
		}
	}

	@Test
	public void testTimeout() throws Exception {
		ChildProcAppHandle handle = null;
		TestClient client = null;

		try {
			//LauncherServer will immediately close the server-side socket when
			// the timeout is set to 0.
			GovindLauncher.launcherConfig.put(CHILD_CONNECTION_TIMEOUT,"0");
			handle = LauncherServer.newAppHandler();

			Socket socket = new Socket(InetAddress.getLoopbackAddress(), LauncherServer.getServerInstance().getPort());
			client = new TestClient(socket);

			//Try a few times since the client-side socket may not reflect the
			// server-side close immediately
			boolean helloSend = false;
			int maxRetries = 10;
			for (int i = 0; i < maxRetries; i++) {
				try {
					if (!helloSend) {
						client.send(new Hello(handle.getSecrect(), "1.6.3"));
						helloSend = true;
					} else {
						client.send(new SetAppId("app-id"));
					}
					fail("由于连接超时，会抛出异常");
				} catch (IllegalStateException | IOException e) {
					break;
				} catch (AssertionError e) {
					if (i < maxRetries - 1) {
						Thread.sleep(100);
					} else {
						throw new AssertionError("超过重试次数：" + maxRetries + "，测试失败");
					}
				}
			}

		} finally {

		}
	}

	private void kill(GovindAppHandle handle) {
		if (handle != null) {
			handle.kill();
		}
	}

	static class TestClient extends LauncherConnection {
		final Thread clientThread;
		final BlockingQueue<Message> inbound;

		public TestClient(Socket socket) throws IOException {
			super(socket);
			this.inbound = new LinkedBlockingQueue<>();
			this.clientThread = new Thread(this);
			clientThread.setName("TestClient");
			clientThread.setDaemon(true);
			clientThread.start();
		}

		@Override
		protected void handle(Message msg) throws IOException {
			inbound.offer(msg);
		}
	}
}
