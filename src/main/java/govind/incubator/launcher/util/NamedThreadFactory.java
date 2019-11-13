package govind.incubator.launcher.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-11
 */
public class NamedThreadFactory implements ThreadFactory {

	final String nameFormat;
	final AtomicInteger threadIds;

	public NamedThreadFactory(String nameFormat	) {
		this.nameFormat = nameFormat;
		this.threadIds = new AtomicInteger();
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread thread = new Thread(r, String.format(nameFormat, threadIds.incrementAndGet()));
		thread.setDaemon(true);
		return thread;
	}
}
