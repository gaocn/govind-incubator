package govind.incubator.launcher;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.logging.Logger;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-11
 *
 * Handles configuring the JUL -> SLF4J bridge.
 *
 */
@Slf4j
public class BaseSuite {
	static final Logger logger = Logger.getLogger("BaseSuite");
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	@Test
	public void testJULLogger() {
		log.info("this is test log");
		logger.info("this is test from j.u.l.Logger");
	}
}
