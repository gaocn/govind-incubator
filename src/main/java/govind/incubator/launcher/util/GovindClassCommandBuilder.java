package govind.incubator.launcher.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static govind.incubator.launcher.util.CommandBuilderUtils.addPermGenSizeOpt;
import static govind.incubator.launcher.util.CommandBuilderUtils.firstNonEmpty;
import static govind.incubator.launcher.util.LauncherConsts.DEFAULT_MEM;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-8
 *
 * command builder for internal spark class.
 *
 * This class handles building the command to launch all internal Spark classes.
 *
 */
public class GovindClassCommandBuilder extends CommandBuilder {
	final String className;
	final List<String> classArgs;

	public GovindClassCommandBuilder(String className, List<String> classArgs) {
		this.className = className;
		this.classArgs = classArgs;
	}

	@Override
	public List<String> buildCommand(Map<String, String> env) throws IOException {
		List<String> javaOptsKeys = new ArrayList<>();
		String memKey;
		String extraClassPath = null;

		if (className.equals("org.apache.spark.deploy.master.Master")) {
			javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
			javaOptsKeys.add("SPARK_MASTER_OPTS");
			memKey = "SPARK_DAEMON_MEMORY";
		} else if (className.equals("org.apache.spark.deploy.worker.Worker")) {
			javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
			javaOptsKeys.add("SPARK_WORKER_OPTS");
			memKey = "SPARK_DAEMON_MEMORY";
		}else {
			javaOptsKeys.add("SPARK_JAVA_OPTS");
			memKey = "SPARK_DRIVER_MEMORY";
		}

		List<String> cmd = buildJavaCommoand(extraClassPath);
		for (String key : javaOptsKeys) {
			addOptionString(cmd, System.getenv(key));
		}

		String mem = firstNonEmpty(memKey!=null?getEnv(memKey):null, DEFAULT_MEM);
		cmd.add("-Xms" + mem);
		cmd.add("-Xmx" + mem);
		addPermGenSizeOpt(cmd);
		cmd.add(className);
		cmd.addAll(classArgs);
		return cmd;
	}
}
