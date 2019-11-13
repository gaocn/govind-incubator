package govind.incubator.launcher.util;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static govind.incubator.launcher.util.CommandBuilderUtils.isEmpty;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-8
 *
 * Parser for spark-submit command line options.
 *
 */
public class GovindSubmitOptionParser {

	// The following constants define the "main" name for the available options. They're defined
	// to avoid copy & paste of the raw strings where they're needed.
	//
	// The fields are not static so that they're exposed to Scala code that uses this class. See
	// SparkSubmitArguments.scala. That is also why this class is not abstract - to allow code to
	// easily use these constants without having to create dummy implementations of this class.
	public final String CLASS = "--class";
	public final String CONF = "--conf";
	public final String DEPLOY_MODE = "--deploy-mode";
	public final String DRIVER_CLASS_PATH = "--driver-class-path";
	public final String DRIVER_CORES = "--driver-cores";
	public final String DRIVER_JAVA_OPTIONS =  "--driver-java-options";
	public final String DRIVER_LIBRARY_PATH = "--driver-library-path";
	public final String DRIVER_MEMORY = "--driver-memory";
	public final String EXECUTOR_MEMORY = "--executor-memory";
	public final String FILES = "--files";
	public final String JARS = "--jars";
	public final String KILL_SUBMISSION = "--kill";
	public final String MASTER = "--master";
	public final String NAME = "--name";
	public final String PACKAGES = "--packages";
	public final String PACKAGES_EXCLUDE = "--exclude-packages";
	public final String PROPERTIES_FILE = "--properties-file";
	public final String PROXY_USER = "--proxy-user";
	public final String PY_FILES = "--py-files";
	public final String REPOSITORIES = "--repositories";
	public final String STATUS = "--status";
	public final String TOTAL_EXECUTOR_CORES = "--total-executor-cores";

	// Options that do not take arguments.
	public final String HELP = "--help";
	public final String SUPERVISE = "--supervise";
	public final String USAGE_ERROR = "--usage-error";
	public final String VERBOSE = "--verbose";
	public final String VERSION = "--version";

	// Standalone-only options.

	// YARN-only options.
	public final String ARCHIVES = "--archives";
	public final String EXECUTOR_CORES = "--executor-cores";
	public final String KEYTAB = "--keytab";
	public final String NUM_EXECUTORS = "--num-executors";
	public final String PRINCIPAL = "--principal";
	public final String QUEUE = "--queue";


	/**
	 * This is the canonical list of spark-submit options. Each entry in
	 * the array contains the different aliases for the same option; the
	 * first element of each entry is the "official" name of the option,
	 * passed to {@link #handle(String, String)}.
	 *
	 * Options not listed here nor in the "switch" list below will result
	 * in a call to {@link #handleUnknown(String)}.
	 *
	 */
	public final String[][] opts = {
			{ ARCHIVES },
			{ CLASS },
			{ CONF, "-c" },
			{ DEPLOY_MODE },
			{ DRIVER_CLASS_PATH },
			{ DRIVER_CORES },
			{ DRIVER_JAVA_OPTIONS },
			{ DRIVER_LIBRARY_PATH },
			{ DRIVER_MEMORY },
			{ EXECUTOR_CORES },
			{ EXECUTOR_MEMORY },
			{ FILES },
			{ JARS },
			{ KEYTAB },
			{ KILL_SUBMISSION },
			{ MASTER },
			{ NAME },
			{ NUM_EXECUTORS },
			{ PACKAGES },
			{ PACKAGES_EXCLUDE },
			{ PRINCIPAL },
			{ PROPERTIES_FILE },
			{ PROXY_USER },
			{ PY_FILES },
			{ QUEUE },
			{ REPOSITORIES },
			{ STATUS },
			{ TOTAL_EXECUTOR_CORES },
	};

	/**
	 * List of switches (command line options that do not take parameters)
	 * recognized by spark-submit.
	 */
	public final String[][] switches = {
			{ HELP, "-h" },
			{ SUPERVISE },
			{ USAGE_ERROR },
			{ VERBOSE, "-v" },
			{ VERSION },
	};


	/**
	 * Callback for when an option with an argument is parsed.
	 *
	 * @param opt The long name of the cli option (might differ from actual command line).
	 * @param value The value. This will be <i>null</i> if the option does not take a value.
	 * @return Whether to continue parsing the argument list.
	 */
	public boolean handle(String opt, String value) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Callback for when an unrecognized option is parsed.
	 *
	 * @param opt Unrecognized option from the command line.
	 * @return Whether to continue parsing the argument list.
	 */
	public boolean handleUnknown(String opt) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Callback for remaining command line arguments after either
	 * {@link #handle(String, String)} or {@link #handleUnknown(String)}
	 * return "false". This will be called at the end of parsing even when
	 * there are no remaining arguments.
	 *
	 * @param extra List of remaining arguments.
	 */
	public void handleExtraArgs(List<String> extra) {
		throw new UnsupportedOperationException();
	}

	/** submit options regex */
	final Pattern pattern = Pattern.compile("(--[^=]+)=(.+)");

	/**
	 * Parse a list of spark-submit command line options.
	 *
	 * @throws IllegalArgumentException if an error is found during parsing
	 */
	public final void parse(List<String> args)  {
		int idx = 0;
		for (idx = 0; idx < args.size(); idx++) {
			String arg = args.get(idx);
			String value = null;

			Matcher matcher = pattern.matcher(arg);
			if (matcher.matches()) {
				arg = matcher.group(1);
				value = matcher.group(2);
			}

			//look for options with a value
			String name = findCliOption(arg, opts);
			if (name != null) {
				if (value == null) {
					if (idx == args.size() - 1) {
						throw new IllegalArgumentException(
								String.format("Missing argument for option '%s'.", arg)
						);
					}
					idx++;
					value = args.get(idx);
				}
				if (!handle(name, value)) {
					break;
				}
				continue;
			}

			//Look for a switch.
			name = findCliOption(arg, switches);
			if (name != null) {
				if (!handle(name, null)) {
					break;
				}
				continue;
			}

			if (!handleUnknown(arg)) {
				break;
			}
		}

		if (idx < args.size()) {
			idx++;
		}
		handleExtraArgs(args.subList(idx, args.size()));
	}

	private String findCliOption(String arg, String[][] opts) {
		for (String[] candidates : opts) {
			for (String candidate : candidates) {
				if (candidate.equals(arg)) {
					return candidates[0];
				}
			}
		}
		return null;
	}

}
