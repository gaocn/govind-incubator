package govind.incubator.launcher.util;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-11
 * <p>
 * 将InputStream中的内容重定向到SLF4J Logger中（级别为：INFO）
 */
public class OutputRedirector {
	final BufferedReader reader;
	final Thread thread;
	final Logger sink;

	private volatile boolean active;

	public OutputRedirector(InputStream in, ThreadFactory tf) {
		this(in, OutputRedirector.class.getName(), tf);
	}

	public OutputRedirector(InputStream in, String loggerName, ThreadFactory tf) {
		this.active = true;
		this.reader = new BufferedReader(new InputStreamReader(in));
		this.thread = tf.newThread(() -> {
			redirect();
		});
		this.sink = Logger.getLogger(loggerName);
		thread.start();
	}

	private void redirect() {
		try {
			String line;
			while ((line = reader.readLine()) != null) {
				if (active) {
					sink.info(line.replace("\\s*$", ""));
				}
			}
		} catch (IOException e) {
			sink.log(Level.FINE, "读取子进程的输出信息失败", e);
		} finally {
		}
	}

	/**
	 * This method just stops the output of the process from showing up
	 * in the local logs. The child's output will still be read (and, thus,
	 * the redirect thread will still be alive) to avoid the child process
	 * hanging because of lack of output buffer.
	 */
	public void stop() {
		active = true;
	}
}
