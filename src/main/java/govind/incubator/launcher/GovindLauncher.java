package govind.incubator.launcher;

import govind.incubator.launcher.GovindAppHandle.Listener;
import govind.incubator.launcher.util.GovindSubmitCommandBuilder;
import govind.incubator.launcher.util.GovindSubmitOptionParser;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static govind.incubator.launcher.util.CommandBuilderUtils.*;
import static govind.incubator.launcher.util.LauncherConsts.CHILD_PROCESS_LOGGER_NAME;
import static govind.incubator.launcher.util.LauncherConsts.ENV_SPARK_HOME;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-7
 * <p>
 * 用于启动Spark应用程序
 * <p>
 * 采用builder设计模式实现对应用程序的配置，并将其作为子进程启动。
 */
@Slf4j
public class GovindLauncher {
	/**
	 * 用于生成唯一的logger名称
	 */
	static final AtomicInteger COUNTER = new AtomicInteger();
	/**
	 * launcher 配置
	 */
	static final Map<String, String> launcherConfig = new HashMap<>();

	final GovindSubmitCommandBuilder builder;

	public GovindLauncher() {
		this(null);
	}

	public GovindLauncher(Map<String, String> env) {
		this.builder = new GovindSubmitCommandBuilder();
		if (env != null) {
			this.builder.childEnv.putAll(env);
		}
	}

	public GovindLauncher setJavaHome(String javaHome) {
		checkNotNull(javaHome, "javaHome");
		builder.javaHome = javaHome;
		return this;
	}

	public GovindLauncher setSparkHome(String sparkHome) {
		checkNotNull(sparkHome, "sparkHome");
		builder.childEnv.put(ENV_SPARK_HOME, sparkHome);
		return this;
	}

	public GovindLauncher setPropertiesFile(String path) {
		checkNotNull(path, "path");
		builder.propertiesFile = path;
		return this;
	}

	public GovindLauncher setAppName(String appName) {
		checkNotNull(appName, "appname");
		builder.appName = appName;
		return this;
	}

	public GovindLauncher setConf(String key, String value) {
		checkNotNull(key, "key");
		checkNotNull(value, "value");
		checkArgument(key.startsWith("govind."), "'key' must start with 'govind'");
		builder.conf.put(key, value);
		return this;
	}

	public GovindLauncher setMaster(String master) {
		checkNotNull(master, "master");
		builder.master = master;
		return this;
	}

	public GovindLauncher setDeployMode(String deployMode) {
		checkNotNull(deployMode, "deployMode");
		builder.deployMode = deployMode;
		return this;
	}

	public GovindLauncher setAppResource(String appResource) {
		checkNotNull(appResource, "appResource");
		builder.appResources = appResource;
		return this;
	}

	public GovindLauncher setMainClass(String mainClass) {
		checkNotNull(mainClass, "mainClass");
		builder.mainClass = mainClass;
		return this;
	}

	public GovindLauncher addSparkArgs(String arg) {
		GovindSubmitOptionParser validator = new ArgumetnValidator(false);
		validator.parse(Arrays.asList(arg));
		builder.sparkArgs.add(arg);
		return this;
	}

	public GovindLauncher addSparkArgs(String name, String value) {
		GovindSubmitOptionParser validator = new ArgumetnValidator(false);
		if (validator.MASTER.equals(name)) {
			setMaster(value);
		} else if ((validator.PROPERTIES_FILE.equals(name))) {
			setPropertiesFile(value);
		} else if (validator.CONF.equals(name)) {
			String[] vals = value.split("=", 2);
			setConf(vals[0], vals[1]);
		} else if (validator.CLASS.equals(name)) {
			setMainClass(value);
		} else if (validator.JARS.equals(name)) {
			builder.jars.clear();
			for (String jar : value.split(",")) {
				addJar(jar);
			}
		} else if (validator.FILES.equals(name)) {
			builder.files.clear();
			for (String file : value.split(",")) {
				addFile(file);
			}
		} else {
			validator.parse(Arrays.asList(name, value));
			builder.sparkArgs.add(name);
			builder.sparkArgs.add(value);
		}
		return this;
	}

	public GovindLauncher addFile(String file) {
		checkNotNull(file, "file");
		builder.files.add(file);
		return this;
	}

	public GovindLauncher addJar(String jar) {
		checkNotNull(jar, "jar");
		builder.jars.add(jar);
		return this;
	}

	public GovindLauncher addAppArgs(String... args) {
		for (String arg : args) {
			checkNotNull(arg, "arg");
			builder.appArgs.add(arg);
		}
		return this;
	}

	public GovindLauncher setVerbose(boolean verbose) {
		builder.verbose = verbose;
		return this;
	}

	/**
	 * Launches a sub-process that will start the configured Spark application.
	 *
	 * @return A process handle for the Spark app.
	 */
	public Process launch() throws IOException {
		return createBuilder().start();
	}

	private ProcessBuilder createBuilder() {
		List<String> cmd = new ArrayList<>();
		String script = isWindows() ? "spark-submit.cmd" : "spark-submit";
		cmd.add(join(File.separator, builder.getSparkHome(), "bin", script));
		cmd.addAll(builder.buildSparkSubmitArgs());

		//// Since the child process is a batch script, let's quote things so that
		// special characters are preserved, otherwise the batch interpreter will
		// mess up the arguments. Batch scripts are weird.
		if (isWindows()) {
			List<String> winCmd = new ArrayList<>();
			for (String arg : cmd) {
				winCmd.add(quoteForBatchScript(arg));
			}
			cmd = winCmd;
		}
		log.debug("cmd: " + join(" ", cmd));

		ProcessBuilder pb = new ProcessBuilder(cmd.toArray(new String[cmd.size()]));
		builder.childEnv.entrySet().forEach(e -> {
			pb.environment().put(e.getKey(), e.getValue());
		});

		return pb;
	}

	/**
	 * Starts a Spark application.
	 * <p>
	 * This method returns a handle that provides information about the running
	 * application and can be used to do basic interaction with it.
	 * <p>
	 * The returned handle assumes that the application will instantiate a single
	 * SparkContext during its lifetime. Once that context reports a final state
	 * (one that indicates the SparkContext has stopped), the handle will not perform
	 * new state transitions, so anything that happens after that cannot be monitored.
	 * If the underlying application is launched as a child process,
	 * {@link GovindAppHandle#kill()} can still be used to kill the child process.
	 * <p>
	 * Currently, all applications are launched as child processes. The child's stdout
	 * and stderr are merged and written to a logger (see <code>java.util.logging</code>).
	 * The logger's name can be defined by setting
	 * {@link govind.incubator.launcher.util.LauncherConsts#CHILD_PROCESS_LOGGER_NAME} in
	 * the app's configuration. If  that option is not set, the code will try to derive a
	 * name from the application's name or main class / script file. If those cannot be
	 * determined, an internal, unique name will be used. In all cases, the logger name
	 * will start with "org.apache.spark.launcher.app", to fit more easily into the
	 * configuration of commonly-used logging systems.
	 *
	 * @param listeners Listeners to add to the handle before the app is launched.
	 * @return A handle for the launched application.
	 */
	public GovindAppHandle startApplication(GovindAppHandle.Listener... listeners) throws IOException {
		ChildProcAppHandle handle = LauncherServer.newAppHandler();
		for (Listener listener : listeners) {
			handle.addListener(listener);
		}

		String appName = builder.getEffectiveConfig().get(CHILD_PROCESS_LOGGER_NAME);
		if (appName == null) {
			if (builder.appName != null) {
				appName = builder.appName;
			} else if (builder.mainClass != null) {
				int dot = builder.mainClass.lastIndexOf(".");
				if (dot >= 0 && dot < builder.mainClass.length() - 1) {
					appName = builder.mainClass.substring(dot + 1);
				} else {
					appName = builder.mainClass;
				}
			}
		} else if (builder.appResources != null) {
			appName = new File(builder.appResources).getName();
		} else {
			appName = String.valueOf(COUNTER.incrementAndGet());
		}


		String loggerPrefix = getClass().getPackage().getName();
		String loggerName = String.format("%s.app.%s", loggerPrefix, appName);

		ProcessBuilder pb = createBuilder().redirectErrorStream(true);
		pb.environment().put(LauncherProtocol.ENV_LAUNCHER_PORT, String.valueOf(LauncherServer.getServerInstance().getPort()));
		pb.environment().put(LauncherProtocol.ENV_LAUNCHER_SECRET, handle.getSecrect());

		try {
			handle.setChildProc(pb.start(), loggerName);
		} catch (IOException e) {
			handle.kill();
			throw e;
		}
		return handle;
	}


	class ArgumetnValidator extends GovindSubmitOptionParser {
		private final boolean hasValue;

		public ArgumetnValidator(boolean hasValue) {
			this.hasValue = hasValue;
		}

		@Override
		public void handleExtraArgs(List<String> extra) {
			//Nop
		}

		@Override
		public boolean handleUnknown(String opt) {
			return true;
		}

		@Override
		public boolean handle(String opt, String value) {
			if (value == null && hasValue) {
				throw new IllegalArgumentException(
						String.format("'%s' does not expect a value", opt)
				);
			}
			return true;
		}
	}
}
