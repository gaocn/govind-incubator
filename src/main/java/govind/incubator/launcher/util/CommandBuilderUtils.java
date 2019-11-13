package govind.incubator.launcher.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-7
 * <p>
 * 提供用于解析配置项所需要的函数
 */
public class CommandBuilderUtils {

	static enum JavaVendor {
		ORACLE, IBM, OPEN_JDK, UNKNOWN;
	}

	public static boolean isEmpty(String s) {
		return s == null || s.isEmpty();
	}

	/**
	 * 用指定分隔符连接字符串
	 */
	public static String join(String separator, String... elements) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < elements.length; i++) {
			if (!isEmpty(elements[i])) {

				if (i == elements.length - 1) {
					sb.append(elements[i]);
				} else {
					sb.append(elements[i]).append(separator);
				}
			}
		}
		return sb.toString();
	}

	public static String join(String separator, Iterable<String> iter) {
		StringBuilder sb = new StringBuilder();

		Iterator<String> iterator = iter.iterator();
		while (iterator.hasNext()) {
			if (sb.length() > 0) {
				sb.append(separator);
			}
			sb.append(iterator.next());
		}
		return sb.toString();
	}

	/** 返回第一个在map中的key对应value不为空的值，若无则返回 null */
	public static String firstNonEmptyValue(String key, Map<?,?>... maps) {
		for (Map<?, ?> map : maps) {
			String value = (String) map.get(key);
			if (!isEmpty(value)) {
				return value;
			}
		}
		return null;
	}
	/** 返回第一个不为空的值，若无则返回 null */
	static String firstNonEmpty(String ... candidates) {
		for (String candidate : candidates) {
			if (!isEmpty(candidate)) {
				return candidate;
			}
		}
		return null;
	}

	public static JavaVendor getJavaVendor() {
		String vendor = System.getProperty("java.vendor");
		if (vendor.contains("Oracle")) {
			return JavaVendor.ORACLE;
		}

		if (vendor.contains("IBM")) {
			return JavaVendor.IBM;
		}

		if (vendor.contains("OpenJDK")) {
			return JavaVendor.OPEN_JDK;
		}
		return JavaVendor.UNKNOWN;
	}

	public static boolean isWindows() {
		String os = System.getProperty("os.name");
		return os.startsWith("Windows");
	}

	/**
	 * 获取环境变量名称
	 */
	public static String getLibPathEnvName() {
		if (isWindows()) {
			return "PATH";
		}

		String os = System.getProperty("os.name");
		if (os.startsWith("Mac OS X")) {
			return "DYLD_LIBRARY_PATH";
		} else {
			return "LD_LIBRARY_PATH";
		}
	}

	/**
	 * 将给定的pathList添加到有的环境变量配置中
	 */
	public static void mergeEnvPathList(Map<String, String> userEnv, String envKey, String pathList) {
		if (!isEmpty(pathList)) {
			String current = firstNonEmpty(userEnv.get(envKey), System.getProperty(envKey));
			userEnv.put(envKey, join(File.separator, current, pathList));
		}
	}


	/**
	 * 解析具有bash语义的参数列表
	 *
	 * Input: "\"ab cd\" efgh 'i \" j'"
	 * Output: [ "ab cd", "efgh", "i \" j" ]
	 */
	public static List<String> parseOptionString(String s) {
		List<String> opts = new ArrayList<String>();
		StringBuilder opt = new StringBuilder();
		boolean inOpt = false;
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;
		boolean escapeNext = false;

		// This is needed to detect when a quoted empty string is used as an argument ("" or '').
		boolean hasData = false;

		for (int i = 0; i < s.length(); i++) {
			int c = s.codePointAt(i);
			if (escapeNext) {
				opt.appendCodePoint(c);
				escapeNext = false;
			} else if (inOpt) {
				switch (c) {
					case '\\':
						if (inSingleQuote) {
							opt.appendCodePoint(c);
						} else {
							escapeNext = true;
						}
						break;
					case '\'':
						if (inDoubleQuote) {
							opt.appendCodePoint(c);
						} else {
							inSingleQuote = !inSingleQuote;
						}
						break;
					case '"':
						if (inSingleQuote) {
							opt.appendCodePoint(c);
						} else {
							inDoubleQuote = !inDoubleQuote;
						}
						break;
					default:
						if (!Character.isWhitespace(c) || inSingleQuote || inDoubleQuote) {
							opt.appendCodePoint(c);
						} else {
							opts.add(opt.toString());
							opt.setLength(0);
							inOpt = false;
							hasData = false;
						}
				}
			} else {
				switch (c) {
					case '\'':
						inSingleQuote = true;
						inOpt = true;
						hasData = true;
						break;
					case '"':
						inDoubleQuote = true;
						inOpt = true;
						hasData = true;
						break;
					case '\\':
						escapeNext = true;
						inOpt = true;
						hasData = true;
						break;
					default:
						if (!Character.isWhitespace(c)) {
							inOpt = true;
							hasData = true;
							opt.appendCodePoint(c);
						}
				}
			}
		}

		checkArgument(!inSingleQuote && !inDoubleQuote && !escapeNext, "Invalid option string: %s", s);
		if (hasData) {
			opts.add(opt.toString());
		}
		return opts;
	}
	/** Throws IllegalArgumentException if the given object is null. */
	public static void checkNotNull(Object o, String arg) {
		if (o == null) {
			throw new IllegalArgumentException(String.format("'%s' must not be null.", arg));
		}
	}

	/** Throws IllegalArgumentException with the given message if the check is false. */
	public static void checkArgument(boolean check, String msg, Object... args) {
		if (!check) {
			throw new IllegalArgumentException(String.format(msg, args));
		}
	}

	/** Throws IllegalStateException with the given message if the check is false. */
	public static void checkState(boolean check, String msg, Object... args) {
		if (!check) {
			throw new IllegalStateException(String.format(msg, args));
		}
	}
	/**
	 * Quote a command argument for a command to be run by a Windows batch script,
	 * if the argument needs quoting. Arguments only seem to need quotes in batch
	 * scripts if they have certain special characters, some of which need extra
	 * (and different) escaping.
	 *
	 *  For example:
	 *    original single argument: ab="cde fgh"
	 *    quoted: "ab^=""cde fgh"""
	 */
	public static String quoteForBatchScript(String arg) {
		boolean needsQuotes = false;
		for (int i = 0; i < arg.length(); i++) {
			int c = arg.codePointAt(i);
			if (Character.isWhitespace(c) || c == '"' || c == '=' || c == ',' || c == ';') {
				needsQuotes = true;
				break;
			}
		}
		if (!needsQuotes) {
			return arg;
		}
		StringBuilder quoted = new StringBuilder();
		quoted.append("\"");
		for (int i = 0; i < arg.length(); i++) {
			int cp = arg.codePointAt(i);
			switch (cp) {
				case '"':
					quoted.append('"');
					break;
				default:
					break;
			}
			quoted.appendCodePoint(cp);
		}
		if (arg.codePointAt(arg.length() - 1) == '\\') {
			quoted.append("\\");
		}
		quoted.append("\"");
		return quoted.toString();
	}

	/**
	 * Quotes a string so that it can be used in a command string.
	 * Basically, just add simple escapes. E.g.:
	 *    original single argument : ab "cd" ef
	 *    after: "ab \"cd\" ef"
	 *
	 * This can be parsed back into a single argument by python's
	 * "shlex.split()" function.
	 */
	static String quoteForCommandString(String s) {
		StringBuilder quoted = new StringBuilder().append('"');
		for (int i = 0; i < s.length(); i++) {
			int cp = s.codePointAt(i);
			if (cp == '"' || cp == '\\') {
				quoted.appendCodePoint('\\');
			}
			quoted.appendCodePoint(cp);
		}
		return quoted.append('"').toString();
	}

	/**
	 * Adds the default perm gen size option for Spark if the VM requires
	 * it and the user hasn't set it.
	 */
	public static void addPermGenSizeOpt(List<String> cmd) {
		// Don't set MaxPermSize for IBM Java, or Oracle Java 8 and later.
		if (getJavaVendor() == JavaVendor.IBM) {
			return;
		}
		String[] version = System.getProperty("java.version").split("\\.");
		if (Integer.parseInt(version[0]) > 1 || Integer.parseInt(version[1]) > 7) {
			return;
		}

		for (String arg : cmd) {
			if (arg.startsWith("-XX:MaxPermSize=")) {
				return;
			}
		}

		cmd.add("-XX:MaxPermSize=256m");
	}
}
