package govind.incubator.launcher.util;

import java.util.*;
import java.util.Map.Entry;

import static govind.incubator.launcher.util.CommandBuilderUtils.*;
import static govind.incubator.launcher.util.LauncherConsts.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-8
 *
 * This builder adds command line parsing compatible with SparkSubmit. It
 * handles setting driver-side options and special parsing behavior needed
 * for the special-casing certain internal Spark applications.
 *
 */
public class GovindSubmitCommandBuilder extends CommandBuilder {
	public final List<String> sparkArgs;
	private final boolean printInfo;
	/**
	 * Controls whether mixing spark-submit arguments with app arguments is
	 * allowed. This is needed to parse the command lines for things like
	 * bin/spark-shell, which allows users to mix and match arguments
	 * (e.g. "bin/spark-shell SparkShellArg --master foo").
	 */
	private boolean allowsMixedArguments;

   /** This map must match the class names for available special classes, since
	* this modifies the way command line parsing works. This maps the class name
	* to the resource to use when calling spark-submit.
   */
	private static final Map<String, String> specialClasses = new HashMap<String, String>();
	static {
		specialClasses.put("org.apache.spark.repl.Main", "spark-shell");
		specialClasses.put("org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver",
				"spark-internal");
		specialClasses.put("org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
				"spark-internal");
	}

	public GovindSubmitCommandBuilder() {
		this.sparkArgs = new ArrayList<String>();
		this.printInfo = false;
	}

	public GovindSubmitCommandBuilder(List<String> args) {
		this.sparkArgs = new ArrayList<>();
		List<String> submitArgs = args;

		if (args.size() > 0 && args.get(0).equals(PYSPARK_SHELL)) {
			this.allowsMixedArguments = true;
			appResources = PYSPARK_SHELL_RESOURCE;
			submitArgs = args.subList(1, args.size());
		} else if (args.size() > 0 && args.get(0).equals(SPARKR_SHELL)) {
			this.allowsMixedArguments =  true;
			appResources = SPARKR_SHELL_RESOURCE;
			submitArgs = args.subList(1, args.size());
		} else {
			this.allowsMixedArguments = false;
		}
		DefaultGovindSubmitOptionParser parser = new DefaultGovindSubmitOptionParser();
		parser.parse(submitArgs);
		this.printInfo = parser.infoRequeted;
	}


	@Override
	public List<String> buildCommand(Map<String, String> env) throws Exception {
		// Load the properties file and check whether spark-submit will be running
		// the app's driver or just launching a cluster app. When running the driver,
		// the JVM's argument will be modified to cover the driver's configuration.
		Map<String, String> config = getEffectiveConfig();
		boolean isClientMode = isClientMode(config);
		String extraClassPath = isClientMode ? config.get(DRIVER_EXTRA_CLASSPATH):null;

		List<String> cmd = buildJavaCommoand(extraClassPath);

		if (isThriftServer(mainClass)) {
			addOptionString(cmd,  System.getenv("SPARK_DAEMON_JAVA_OPTS"));
		}
		addOptionString(cmd, System.getenv("SPARK_SUBMIT_OPTS"));
		addOptionString(cmd, System.getenv("SPARK_JAVA_OPTS"));

		if (isClientMode) {
			// Figuring out where the memory value come from is a little tricky
			// due to precedence. Precedence is observed in the following order:
			// - explicit configuration (setConf()), which also covers
			// 				--driver-memory cli argument.
			// - properties file.
			// - SPARK_DRIVER_MEMORY env variable
			// - SPARK_MEM env variable
			// - default value (1g)
			// Take Thrift Server as daemon
			String tsMemory =
					isThriftServer(mainClass) ? System.getenv("SPARK_DAEMON_MEMORY") : null;
			String memory = firstNonEmpty(tsMemory, config.get(DRIVER_MEMOPRY),
					System.getenv("SPARK_DRIVER_MEMORY"), System.getenv("SPARK_MEM"), DEFAULT_MEM);
			cmd.add("-Xms" + memory);
			cmd.add("-Xmx" + memory);
			addOptionString(cmd, config.get(DRIVER_EXTRA_OPTIONS));
			mergeEnvPathList(env, getLibPathEnvName(),
					config.get(DRIVER_EXTRA_LIBRARY_PATH));
		}
		addPermGenSizeOpt(cmd);
		cmd.add("org.apache.spark.deploy.SparkSubmit");
		cmd.addAll(buildSparkSubmitArgs());
		return cmd;
	}

	public List<String> buildSparkSubmitArgs() {
		List<String> args = new ArrayList<>();
		DefaultGovindSubmitOptionParser parser = new DefaultGovindSubmitOptionParser();

		if (verbose) {
			args.add(parser.VERBOSE);
		}

		if (master != null) {
			args.add(parser.MASTER);
			args.add(master);
		}

		if (deployMode != null) {
			args.add(parser.DEPLOY_MODE);
			args.add(deployMode);
		}

		if (appName != null) {
			args.add(parser.NAME);
			args.add(appName);
		}

		for (Entry<String, String> e : conf.entrySet()) {
			args.add(parser.CONF);
			args.add(String.format("%s=%s", e.getKey(), e.getValue()));
		}

		if (propertiesFile != null) {
			args.add(parser.PROPERTIES_FILE);
			args.add(propertiesFile);
		}

		if (!jars.isEmpty()) {
			args.add(parser.JARS);
			args.add(join(",", jars));
		}

		if (!files.isEmpty()) {
			args.add(parser.FILES);
			args.add(join(",", files));
		}

		if (!pyFiles.isEmpty()) {
			args.add(parser.PY_FILES);
			args.add(join(",", pyFiles));
		}

		if (mainClass != null) {
			args.add(parser.CLASS);
			args.add(mainClass);
		}

		args.addAll(sparkArgs);
		if (appResources != null) {
			args.add(appResources);
		}

		args.addAll(appArgs);
		return args;
	}

	class DefaultGovindSubmitOptionParser extends GovindSubmitOptionParser {
		boolean infoRequeted = false;

		@Override
		public boolean handle(String opt, String value) {
			if (opt.equals(MASTER)) {
				master = value;
			} else if(opt.equals(DEPLOY_MODE)) {
				deployMode = value;
			} else if(opt.equals(PROPERTIES_FILE)) {
				propertiesFile = value;
			} else if(opt.equals(DRIVER_MEMORY)) {
				conf.put(LauncherConsts.DRIVER_MEMOPRY, value);
			} else if (opt.equals(DRIVER_CLASS_PATH)) {
				conf.put(LauncherConsts.DRIVER_EXTRA_CLASSPATH, value);
			} else if (opt.equals(DRIVER_JAVA_OPTIONS)) {
				conf.put(LauncherConsts.DRIVER_EXTRA_OPTIONS, value);
			} else if (opt.equals(DRIVER_LIBRARY_PATH)) {
				conf.put(LauncherConsts.DRIVER_EXTRA_LIBRARY_PATH, value);
			} else if(opt.equals(CONF)) {
				String[] split = value.split("=", 2);
				checkArgument(split.length == 2, "Invalid argument to %s: %s", CONF, value);
				conf.put(split[0],  split[1]);
			} else if (opt.equals(CLASS)) {
				// The special classes require some special command line handling,
				// since they allow mixing spark-submit arguments with arguments
				// that should be propagated to the shell itself. Note that for this
				// to work, the "--class" argument must come before any non-spark-submit
				// arguments.
				mainClass = value;
				if (specialClasses.containsKey(value)) {
					allowsMixedArguments = true;
					appResources = specialClasses.get(value);
				}
			}else if (opt.equals(HELP) || opt.equals(USAGE_ERROR)) {
				infoRequeted = true;
				sparkArgs.add(opt);
			} else if (opt.equals(VERSION)) {
				infoRequeted = true;
				sparkArgs.add(opt);
			} else {
				sparkArgs.add(opt);
				if (value != null) {
					sparkArgs.add(value);
				}
			}
			return true;
		}

		@Override
		public boolean handleUnknown(String opt) {
			//当allowsMixedArguments=true时未识别的参数作为用户参数，
			// 否则未识别的参数作为应用程序参数并且剩下的都为应用程序参数
			if (allowsMixedArguments) {
				appArgs.add(opt);
				return true;
			} else {
				checkArgument(opt.startsWith("--"), "无法识别的选项：" +  opt);
				sparkArgs.add(opt);
				return false;
			}
		}

		@Override
		public void handleExtraArgs(List<String> extra) {
			sparkArgs.addAll(extra);
		}
	}
	private boolean isClientMode(Map<String, String> userProps) {
		String userMaster = firstNonEmpty(master, userProps.get(GOVIND_MASTER));
		// Default master is "local[*]", so assume client mode in that case.
		return userMaster == null
				|| userMaster.startsWith("local")
				|| "client".equals(userMaster)
				|| (userMaster.equals("yarn-client"));
	}

	/**
	 * Return whether the given main class represents a thrift server.
	 */
	private boolean isThriftServer(String mainClass) {
		return (mainClass != null &&
				mainClass.equals("org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"));
	}

}
