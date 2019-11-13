package govind.incubator.launcher;

import govind.incubator.launcher.util.CommandBuilderUtils;
import govind.incubator.launcher.util.GovindSubmitCommandBuilder;
import govind.incubator.launcher.util.GovindSubmitOptionParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import static govind.incubator.launcher.util.CommandBuilderUtils.getLibPathEnvName;
import static govind.incubator.launcher.util.CommandBuilderUtils.join;
import static govind.incubator.launcher.util.LauncherConsts.*;
import static org.junit.Assert.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-12
 */
public class GovindSubmitCommandBuilderSuite {
	private static File dummyPropsFile;
	private static GovindSubmitOptionParser parser;

	@BeforeClass
	public static void beforeAll() throws IOException {
		dummyPropsFile = File.createTempFile("govid", "properties");
		parser = new GovindSubmitOptionParser();
		System.setProperty("govind.test.home", "E:\\EclipseWPFroWin10\\govind-incubator");
	}

	@AfterClass
	public static void afterAll() {
		dummyPropsFile.delete();
		System.clearProperty("govind.test.home");
	}

	@Test
	public void testDriverCmdBuilder() throws Exception {
		/**
		 * C:\Program Files\Java\jdk1.8.0_102\jre\bin\java
		 * 	 -cp /driver;E:\EclipseWPFroWin10\govind-incubator/src/test/java/resources\;
		 * 	     E:\EclipseWPFroWin10\govind-incubator\assembly\
		 * 	 -Xms1g
		 * 	 -Xmx1g
		 * 	 org.apache.spark.deploy.SparkSubmit
		 * 	   --master yarn
		 * 	   --deploy-mode client
		 * 	   --name MyApp
		 * 	   --conf govind.foo=foo
		 * 	   --class my.class /foo foo bar
		 */
		testCmdBuilder(true, true);

		/**
		 * C:\Program Files\Java\jdk1.8.0_102\jre\bin\java
		 *   -cp /driver;E:\EclipseWPFroWin10\govind-incubator\conf;E:\EclipseWPFroWin10\
		 *       govind-incubator\assembly\
		 *   -Xms1g
		 *   -Xmx1g
		 *   org.apache.spark.deploy.SparkSubmit
		 *   --master yarn
		 *   --deploy-mode client
		 *   --name MyApp
		 *   --conf govind.foo=foo
		 *   --conf govind.driver.memory=1g
		 *   --conf govind.driver.extraJavaOptions=-Ddriver
		 *   -XX:MaxPermSize=256m
		 *   --conf govind.driver.extraLibraryPath=/native
		 *   --conf govind.driver.extraClassPath=/driver
		 *   --properties-file C:\Users\高文文\AppData\Local\Temp\
		 *        govid4239054972195762930properties
		 *    --class my.class /foo foo bar
		 */
		testCmdBuilder(true, false);
	}

	@Test
	public void  testClusterCmdBuilder() throws Exception {
		testCmdBuilder(false, true);
		testCmdBuilder(false, false);
	}

	@Test
	public void testCliParser() throws Exception {
		List<String> args = Arrays.asList(
				parser.MASTER, "local",
				parser.DRIVER_MEMORY, "42g",
				parser.DRIVER_CLASS_PATH, "/driverCP",
				parser.DRIVER_JAVA_OPTIONS, "/driverOpts",
				parser.CONF, "govind.randomOption=foo",
				parser.CONF, DRIVER_EXTRA_LIBRARY_PATH + "=/driverLibPath");

		HashMap<String, String> env = new HashMap<>();
		List<String> cmd = buildCommand(args, env);

		assertTrue(findInStringList(env.get(getLibPathEnvName()), File.pathSeparator, "/driverLibPath"));
		assertTrue(findInStringList(findArgValue(cmd, "-cp"), File.pathSeparator, "/driverCP"));
		assertTrue("需要配置-Xms参数", cmd.contains("-Xms42g"));
		assertTrue("需要配置-Xmx参数", cmd.contains("-Xmx42g"));
		assertTrue("命令行中应该含有用户自定义配置参数", Collections.indexOfSubList(cmd, Arrays.asList(parser.CONF, "govind.randomOption=foo"))>0);
	}

	@Test
	public void testShellCliParser() {
		List<String> args = Arrays.asList(
				parser.CLASS, "org.apache.spark.repl.Main",
				parser.MASTER, "foo",
				"--app-arg", "bar",
				"--app-switch",
				parser.FILES, "baz",
				parser.NAME, "appName"
		);

		List<String> appArgs = newCommandBuilder(args).buildSparkSubmitArgs();
		List<String> expected = Arrays.asList("spark-shell", "--app-arg", "bar", "--app-switch");
		assertEquals(expected, appArgs.subList(appArgs.size()-expected.size(), appArgs.size()));

	}

	private void testCmdBuilder(boolean isDriver, boolean useDefaultPropertyFile) throws Exception {
		String deployMode = isDriver ? "client" : "cluster";

		GovindSubmitCommandBuilder launcher = newCommandBuilder(Collections.emptyList());

		launcher.childEnv.put(ENV_SPARK_HOME, System.getProperty("govind.test.home"));
		launcher.master = "yarn-" + deployMode;
		launcher.deployMode = deployMode;
		launcher.appResources = "/foo";
		launcher.appName = "MyApp";
		launcher.mainClass = "my.class";
		launcher.appArgs.add("foo");
		launcher.appArgs.add("bar");
		launcher.conf.put("govind.foo", "foo");

		if (!useDefaultPropertyFile) {
			launcher.propertiesFile = dummyPropsFile.getAbsolutePath();
			launcher.conf.put(DRIVER_MEMOPRY, "1g");
			launcher.conf.put(DRIVER_EXTRA_CLASSPATH, "/driver");
			launcher.conf.put(DRIVER_EXTRA_OPTIONS, "-Ddriver -XX:MaxPermSize=256m");
			launcher.conf.put(DRIVER_EXTRA_LIBRARY_PATH, "/native");
		} else {
			launcher.childEnv.put("SPARK_CONF_DIR", System.getProperty("govind.test.home")
					+ "/src/test/java/resources");
		}

		Map<String, String> env = new HashMap<>();
		List<String> cmd = launcher.buildCommand(env);

		if (isDriver) {
			assertTrue("Driver -Xms should be configured.", cmd.contains("-Xms1g"));
			assertTrue("Driver -Xmx should be configured.", cmd.contains("-Xmx1g"));
		} else {
			boolean found = false;
			for (String c : cmd) {
				if (c.startsWith("-Xms") || c.startsWith("-Xmx")) {
					found = true;
					break;
				}
			}
			assertFalse("memory arguments should not be set", found);
		}

		for (String arg : cmd) {
			if (arg.startsWith("-XX:MaxPermSize=")) {
				if (isDriver) {
					assertEquals("-XX:MaxPermSize=256m", arg);
				} else {
					assertEquals("-XX:MaxPermSize=256m", arg);
				}
			}
		}

		String[] cp = findArgValue(cmd, "-cp").split(Pattern.quote(File.pathSeparator));
		if (isDriver) {
			assertTrue("Driver classpath should contain provided entry.", contains("/driver", cp));
		} else {
			assertFalse("Driver classpath should not be in command.", contains("/driver", cp));
		}

		String libPath = env.get(CommandBuilderUtils.getLibPathEnvName());
		if (isDriver) {
			assertNotNull("Native library path should be set", libPath);
			assertTrue("Native library path should contain provided entry", contains("/native", libPath.split(File.pathSeparator)));
		} else {
			assertNull("Native library should not be set", libPath);
		}

		if (!useDefaultPropertyFile) {
			assertEquals(dummyPropsFile.getAbsolutePath(), findArgValue(cmd, parser.PROPERTIES_FILE));
		}

		assertEquals("yarn-" + deployMode, findArgValue(cmd, parser.MASTER));
		assertEquals(deployMode, findArgValue(cmd, parser.DEPLOY_MODE));
		assertEquals("my.class", findArgValue(cmd, parser.CLASS));
		assertEquals("MyApp", findArgValue(cmd, parser.NAME));

		boolean appArgsOk = false;
		for (int i = 0; i < cmd.size(); i++) {
			if (cmd.get(i).equals("/foo")) {
				assertEquals("foo", cmd.get(i+1));
				assertEquals("bar", cmd.get(i+2));
				assertEquals(cmd.size(), i+3);
				appArgsOk = true;
				break;
			}
		}
		assertTrue("App resource and args should be added to command.", appArgsOk);

		Map<String, String> conf = parseConf(cmd, parser);
		assertEquals("foo", conf.get("govind.foo"));
		System.out.println(join(" ", cmd));
	}


	private boolean contains(String needle, String[] haystack) {
		for (String e : haystack) {
			if (e.equals(needle)) {
				return true;
			}
		}
		return false;
	}

	private Map<String, String> parseConf(List<String> cmd, GovindSubmitOptionParser parser) {
		Map<String, String> conf = new HashMap<>();
		for (int i = 0; i < cmd.size(); i++) {
			if (cmd.get(i).equals(parser.CONF)) {
				String[] val = cmd.get(i + 1).split("=", 2);
				conf.put(val[0], val[1]);
				i += 1;
			}
		}
		return conf;
	}

	private String findArgValue(List<String> cmd, String name) {
		for (int i = 0; i < cmd.size(); i++) {
			if (cmd.get(i).equals(name)) {
				return cmd.get(i + 1);
			}
		}
		fail(String.format("参数%s没有找到", name));
		return null;
	}

	private boolean findInStringList(String list, String sep, String needle) {
		return contains(needle, list.split(sep));
	}

	private GovindSubmitCommandBuilder newCommandBuilder(List<String> args) {
		GovindSubmitCommandBuilder builder = new GovindSubmitCommandBuilder(args);
		builder.childEnv.put(ENV_SPARK_HOME, System.getProperty("govind.test.home"));
		builder.childEnv.put(ENV_SPARK_ASSEMBLY, "dummy");
		builder.childEnv.put("SPARK_ASSEMBLY", "E:\\EclipseWPFroWin10\\govind-incubator\\assembly");
		return builder;
	}

	private List<String> buildCommand(List<String> args, Map<String, String> env) throws Exception {
		return newCommandBuilder(args).buildCommand(env);
	}
}
