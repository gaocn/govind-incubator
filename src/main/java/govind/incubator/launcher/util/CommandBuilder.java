package govind.incubator.launcher.util;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import static govind.incubator.launcher.util.CommandBuilderUtils.*;
import static govind.incubator.launcher.util.LauncherConsts.DEFAULT_PROPERTIES_FILE;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-8
 * <p>
 * Spark应用程序命令行参数抽象类
 */
abstract public class CommandBuilder {
	public boolean verbose;
	public String appName;
	public String appResources;
	public String deployMode;
	public String javaHome;
	public String mainClass;
	public String master;
	public String propertiesFile;
	public final List<String> appArgs;
	public final List<String> jars;
	public final List<String> files;
	public final List<String> pyFiles;
	public Map<String, String> childEnv;
	public final Map<String, String> conf;

	/**
	 * 缓存已解析的应用程序配置， 避免多次读取配置文件、解析配置文件
	 */
	private Map<String, String> effectiveConfig;


	public CommandBuilder() {
		this.appArgs = new ArrayList<>();
		this.jars = new ArrayList<>();
		this.files = new ArrayList<>();
		this.pyFiles = new ArrayList<>();
		this.childEnv = new HashMap<>();
		this.conf = new HashMap<>();
	}


	/**
	 * 构建执行命令
	 *
	 * @param env 包含子进程运行所需要的环境变量
	 * @return
	 * @throws IOException
	 */
	public abstract List<String> buildCommand(Map<String, String> env) throws IOException, Exception;

	/**
	 * 构建执行java命令所需要的参数列表。
	 * <p>
	 * 1、找到 java命令所在位置；
	 * 2、添加java options；
	 * 3、从java-opts文件中加载定义的java options
	 *
	 * @param extraClassPath
	 * @return
	 * @throws IOException
	 */
	List<String> buildJavaCommoand(String extraClassPath) throws IOException {
		List<String> cmd = new ArrayList<>();
		String envJavaHome;

		//java路径
		if (javaHome != null) {
			cmd.add(join(File.separator, javaHome, "bin", "java"));
		} else if ((envJavaHome = System.getProperty("JAVA_HOME")) != null) {
			cmd.add(join(File.separator, envJavaHome, "bin", "java"));
		} else {
			cmd.add(join(File.separator, System.getProperty("java.home"), "bin", "java"));
		}

		//若java-opts文件存在，则从中文件加载java options
		File javaOpts = new File(join(File.separator, getConfDir(), "java-opts"));
		if (javaOpts.isFile()) {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(javaOpts), "utf-8"));
			try {
				String line;
				while ((line = reader.readLine()) != null) {
					addOptionString(cmd, line);
				}
			} finally {
				reader.close();
			}
		}

		cmd.add("-cp");
		cmd.add(join(File.pathSeparator, buildClassPath(extraClassPath)));
		return cmd;
	}


	void addOptionString(List<String> cmd, String options) {
		if (!isEmpty(options)) {
			for (String op : parseOptionString(options)) {
				cmd.add(op);
			}
		}
	}

	/**
	 * Builds the classpath for the application. Returns a list with
	 * one classpath entry per element; each entry is formatted in the
	 * way expected by <i>java.net.URLClassLoader</i> (more specifically,
	 * with trailing slashes for directories).
	 */
	List<String> buildClassPath(String appClassPath) {
		List<String> cp = new ArrayList<>();
		String sparkHome = getSparkHome();

		addToClassPath(cp, getEnv("SPARK_CLASSPATH"));
		addToClassPath(cp, appClassPath);
		addToClassPath(cp, getConfDir());

		boolean prependClasses = !isEmpty(getEnv("SPARK_PREPEND_CLASSES"));
		boolean isTesting = "1".equals(getEnv("SPARK_TESTING"));
		if (prependClasses || isTesting) {
			String scala = getScalaVersion();
			List<String> projects = Arrays.asList(
					"core", "repl", "mllib", "bagel", "graphx",
					"streaming", "tools", "sql/catalyst", "sql/core", "sql/hive",
					"sql/hive-thriftserver", "yarn", "launcher", "network/common",
					"network/shuffle", "network/yarn");
			if (prependClasses) {
				if (!isTesting) {
					System.err.println(
							"NOTE: SPARK_PREPEND_CLASSES is set, placing locally " +
									"compiled Spark classes ahead of assembly.");
				}
				for (String project : projects) {
					addToClassPath(cp,
							String.format("%s/%s/target/scala-%s/classes",
									sparkHome, project, scala));
				}
			}
			if (isTesting) {
				for (String project : projects) {
					addToClassPath(cp,
							String.format("%s/%s/target/scala-%s/test-classes",
									sparkHome, project, scala));
				}
			}

			// Add this path to include jars that are shaded in the final
			// deliverable created during the maven build. These jars are
			// copied to this directory during the build.
			addToClassPath(cp, String.format("%s/core/target/jars/*", sparkHome));
		}

		// We can't rely on the ENV_SPARK_ASSEMBLY variable to be set. Certain
		// situations, such as  when running unit tests, or user code that embeds
		// Spark and creates a SparkContext with a local or local-cluster master,
		// will cause this code to be called from an environment where that env
		// variable is not guaranteed to exist.
		//
		// For the testing case, we rely on the test code to set and propagate the
		// test classpath appropriately.
		//
		// For the user code case, we fall back to looking for the Spark assembly
		// under SPARK_HOME. That duplicates some of the code in the shell scripts
		// that look for the assembly, though.
		String assembly = getEnv("SPARK_ASSEMBLY");
		if (assembly == null && !isTesting) {
			assembly = findAssembly();
		}
		addToClassPath(cp, assembly);

		// Datanucleus jars must be included on the classpath. Datanucleus jars
		// do not work if only included in the uber jar as plugin.xml metadata
		// is lost. Both sbt and maven will populate "lib_managed/jars/" with the
		// datanucleus jars when Spark is built with Hive
		/*
		File libdir;
		if (new File(sparkHome, "RELEASE").isFile()) {
			libdir = new File(sparkHome, "lib");
		} else {
			libdir = new File(sparkHome, "lib_managed/jars");
		}

		if (libdir.isDirectory()) {
			for (File jar : libdir.listFiles()) {
				if (jar.getName().startsWith("datanucleus-")) {
					addToClassPath(cp, jar.getAbsolutePath());
				}
			}
		} else {
			checkState(isTesting, "Library directory '%s' does not exist.", libdir.getAbsolutePath());
		}
		*/

		addToClassPath(cp, getEnv("HADOOP_CONF_DIR"));
		addToClassPath(cp, getEnv("YARN_CONF_DIR"));
		addToClassPath(cp, getEnv("SPARK_DIST_CLASSPATH"));

		return cp;
	}

	public Map<String, String> getEffectiveConfig() throws IOException {
		if (effectiveConfig == null) {
			effectiveConfig = new HashMap<>(conf);
			Properties p = loadPropertiesFile();
			for (String key : p.stringPropertyNames()) {
				if (!effectiveConfig.containsKey(key)) {
					effectiveConfig.put(key, p.getProperty(key));
				}
			}
		}
		return effectiveConfig;
	}

	/**
	 * Loads the configuration file for the application, if it exists.
	 * This is either the user-specified properties file, or the
	 * govind-defaults.conf file under the Spark configuration directory.
	 */
	private Properties loadPropertiesFile() throws IOException {
		Properties props = new Properties();
		File propsFile;
		if (propertiesFile != null) {
			propsFile = new File(propertiesFile);
			checkArgument(propsFile.isFile(), "Invalid properties file '%s'.", propertiesFile);
		} else {
			propsFile = new File(getConfDir(), DEFAULT_PROPERTIES_FILE);
		}

		if (propsFile.isFile()) {
			FileInputStream fd = null;
			try {
				fd = new FileInputStream(propsFile);
				props.load(new InputStreamReader(fd, "UTF-8"));
				for (Map.Entry<Object, Object> e : props.entrySet()) {
					e.setValue(e.getValue().toString().trim());
				}
			} finally {
				if (fd != null) {
					try {
						fd.close();
					} catch (IOException e) {
						// Ignore.
					}
				}
			}
		}

		return props;
	}

	/**
	 * 将classpath添加到cp中
	 *
	 * @param cp
	 * @param entries classpath entries, separated by File.pathSeparator
	 */
	private void addToClassPath(List<String> cp, String entries) {
		if (isEmpty(entries)) {
			return;
		}

		String[] splits = entries.split(Pattern.quote(File.pathSeparator));
		for (String entry : splits) {
			if (new File(entry).isDirectory() && !entry.endsWith(File.separator)) {
				entry += File.separator;
			}
			cp.add(entry);
		}
	}


	protected String getConfDir() {
		String confDir = getEnv("SPARK_CONF_DIR");
		return isEmpty(confDir) ? join(File.separator, getSparkHome(), "conf") : confDir;
	}

	public String getSparkHome() {
		String sparkHome = getEnv("SPARK_HOME");
		checkState(!isEmpty(sparkHome), "没有找到SPARK_HOME，请确保设置了该环境变量");
		return sparkHome;
	}

	protected String getEnv(String key) {
		return firstNonEmpty(childEnv.get(key), System.getenv(key));
	}
	private String findAssembly() {
		String sparkHome = getSparkHome();
		File libdir;
		if (new File(sparkHome, "RELEASE").isFile()) {
			libdir = new File(sparkHome, "lib");
			checkState(libdir.isDirectory(), "Library directory '%s' does not exist.",
					libdir.getAbsolutePath());
		} else {
			libdir = new File(sparkHome, String.format("assembly/target/scala-%s", getScalaVersion()));
		}

		final Pattern re = Pattern.compile("spark-assembly.*hadoop.*\\.jar");
		FileFilter filter = file -> file.isFile() && re.matcher(file.getName()).matches();
		File[] assemblies = libdir.listFiles(filter);
		checkState(assemblies != null && assemblies.length > 0, "No assemblies found in '%s'.", libdir);
		checkState(assemblies.length == 1, "Multiple assemblies found in '%s'.", libdir);
		return assemblies[0].getAbsolutePath();
	}

	String getScalaVersion() {
		String scala = getEnv("SPARK_SCALA_VERSION");
		if (scala != null) {
			return scala;
		}
		String sparkHome = getSparkHome();
		File scala210 = new File(sparkHome, "launcher/target/scala-2.10");
		File scala211 = new File(sparkHome, "launcher/target/scala-2.11");
		checkState(!scala210.isDirectory() || !scala211.isDirectory(),
				"Presence of build for both scala versions (2.10 and 2.11) detected.\n" +
						"Either clean one of them or set SPARK_SCALA_VERSION in your environment.");
		if (scala210.isDirectory()) {
			return "2.10";
		} else {
			checkState(scala211.isDirectory(), "Cannot find any build directories.");
			return "2.11";
		}
	}
}
