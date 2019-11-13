package govind.incubator.launcher;

import govind.incubator.launcher.util.CommandBuilder;
import govind.incubator.launcher.util.GovindClassCommandBuilder;
import govind.incubator.launcher.util.GovindSubmitCommandBuilder;
import govind.incubator.launcher.util.GovindSubmitOptionParser;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;

import static govind.incubator.launcher.util.CommandBuilderUtils.checkArgument;
import static govind.incubator.launcher.util.CommandBuilderUtils.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-11
 *
 *
 * Command line interface for the Spark lauline interface for the Spark launcher.
 * Used internally by Spark scripts.
 *
 */
public class Main {
	/**
	 * Usage: Main [class] [class args]
	 * <p>
	 * This CLI works in two different modes:
	 * <ul>
	 *     <li>
	 *         spark-submit: if <i>class</i> is 'org.apache.spark.deploy.SparkSubmit',
	 *         thr {@link GovindLauncher} class is used to launch a spark application.
	 *     </li>
	 *     <li>
	 *         spark-class: if another class is provided, an internal Spark class is run.
	 *     </li>
	 * </ul>
	 *
	 * This class works in tandem with the "bin/spark-class" script on Unix-like systems,
	 * and "bin/spark-class2.cmd" batch script on Windows to execute the final command.
	 * <p>
	 * On Unix-like systems, the output is a list of command arguments, separated by the
	 * NULL character. On Windows, the output is a command line suitable for direct
	 * execution from the script.
	 *
	 */
	public static void main(String[] args) throws Exception {
		checkArgument(args.length > 0, "输入参数不满足添加：未指定类名");

		ArrayList<String> inArgs = (ArrayList<String>) new ArrayList<>(Arrays.asList(args));
		String className = inArgs.remove(0);

		boolean printLaunchCommand = !isEmpty(System.getenv("SPARK_PRINT_LAUNCH_COMMAND"));
		CommandBuilder builder;
		if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
			try {
				builder = new GovindSubmitCommandBuilder(inArgs);
			} catch (IllegalArgumentException e) {
				printLaunchCommand = false;
				System.err.println("Error: " + e.getMessage());
				System.err.println();

				MainClassOptionParser parser = new MainClassOptionParser();
				try {
					parser.parse(inArgs);
				} catch (Exception e1) {

				}

				List<String> help = new ArrayList<>();
				if (parser.className != null) {
					help.add(parser.CLASS);
					help.add(parser.className);
				}
				help.add(parser.USAGE_ERROR);
				builder = new GovindSubmitCommandBuilder(help);
			}
		} else {
			builder = new GovindClassCommandBuilder(className, inArgs);
		}

		Map<String, String> env = new HashMap<>();
		List<String> cmd = builder.buildCommand(env);
		if (printLaunchCommand) {
			System.err.println("Govind Command: " + join(" ", cmd));
			System.err.println("================================================");
		}

		if (isWindows()) {
			System.out.println(prepareWindowsCommand(cmd, env));
		} else {
			// In bash, use NULL as the arg separator since it cannot be used in an argument.
			List<String> bashCmd = prepareBashCommand(cmd, env);
			for (String c : bashCmd) {
				System.out.println(c);
				System.out.println('\0');
			}
		}
	}

	/**
	 * Prepare a command line for execution from a Windows batch script.
	 *
	 *  The method quotes all arguments so that spaces are handled as expected.
	 *  Quotes within arguments are "double quoted" (which is batch for escaping
	 *  a quote). This page has more details about quoting and other batch script
	 *  fun stuff: http://ss64.com/nt/syntax-esc.html
	 *
	 */
	private static String prepareWindowsCommand(List<String> cmd, Map<String, String> childEnv) {
		StringBuilder cmdLine = new StringBuilder();
		childEnv.entrySet().forEach(e -> {
			cmdLine.append(String.format("set %s=%s", e.getKey(), e.getValue()));
			cmdLine.append(" && ");
		});

		cmd.forEach(arg ->{
			cmdLine.append(quoteForBatchScript(arg));
			cmdLine.append(" ");
		});
		return cmdLine.toString();
	}


	/**
	 *
	 * Prepare the command for execution from a bash script. The final command will
	 * have commands to set up any needed environment variables needed by the child
	 * process.
	 *
	 */
	private static List<String> prepareBashCommand(List<String> cmd, Map<String, String> childEnv) {
		if (childEnv.isEmpty()) {
			return cmd;
		}

		List<String> newCmd = new ArrayList<>();
		newCmd.add("env;");

		childEnv.entrySet().forEach(e->{
			newCmd.add(String.format("%s=%s", e.getKey(), e.getValue()));
		});
		newCmd.addAll(cmd);
		return newCmd;
	}


	/**
	 * A parser used when command line parsing fails for spark-submit. It's used
	 * as a best-effort at trying to identify the class the user wanted to invoke,
	 * since that may require special usage strings (handled by SparkSubmitArguments).
	 */
	static class MainClassOptionParser extends GovindSubmitOptionParser {
		String className;

		@Override
		public boolean handle(String opt, String value) {
			if (CLASS.equals(opt)) {
				className = value;
			}
			return false;
		}

		@Override
		public boolean handleUnknown(String opt) { return false; }

		@Override
		public void handleExtraArgs(List<String> extra) { }
	}
}
