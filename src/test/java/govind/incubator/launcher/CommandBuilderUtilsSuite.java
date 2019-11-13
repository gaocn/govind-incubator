package govind.incubator.launcher;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static govind.incubator.launcher.util.CommandBuilderUtils.parseOptionString;
import static govind.incubator.launcher.util.CommandBuilderUtils.quoteForBatchScript;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-12
 */
public class CommandBuilderUtilsSuite {
	@Test
	public void testValidOptionString() {
		testOpt("a b c d e", Arrays.asList("a", "b", "c", "d", "e"));
		testOpt("a 'b c' \"d\" e", Arrays.asList("a", "b c", "d", "e"));
		testOpt("a 'b\\\"c' \"'d'\" e", Arrays.asList("a", "b\\\"c", "'d'", "e"));
		testOpt("a 'b\"c' \"\\\"d\\\"\" e", Arrays.asList("a", "b\"c", "\"d\"", "e"));
		testOpt(" a b c \\\\ ", Arrays.asList("a","b","c","\\"));

		testOpt("", new ArrayList<String>());
		testOpt("a", Arrays.asList("a"));
		testOpt("aaa", Arrays.asList("aaa"));
		testOpt("a b c", Arrays.asList("a", "b", "c"));
		testOpt("  a   b\t c ", Arrays.asList("a", "b", "c"));
		testOpt("a 'b c'", Arrays.asList("a", "b c"));
		testOpt("a 'b c' d", Arrays.asList("a", "b c", "d"));
		testOpt("'b c'", Arrays.asList("b c"));
		testOpt("a \"b c\"", Arrays.asList("a", "b c"));
		testOpt("a \"b c\" d", Arrays.asList("a", "b c", "d"));
		testOpt("\"b c\"", Arrays.asList("b c"));
		testOpt("a 'b\" c' \"d' e\"", Arrays.asList("a", "b\" c", "d' e"));
		testOpt("a\t'b\nc'\nd", Arrays.asList("a", "b\nc", "d"));
		testOpt("a \"b\\\\c\"", Arrays.asList("a", "b\\c"));
		testOpt("a \"b\\\"c\"", Arrays.asList("a", "b\"c"));
		testOpt("a 'b\\\"c'", Arrays.asList("a", "b\\\"c"));
		testOpt("'a'b", Arrays.asList("ab"));
		testOpt("'a''b'", Arrays.asList("ab"));
		testOpt("\"a\"b", Arrays.asList("ab"));
		testOpt("\"a\"\"b\"", Arrays.asList("ab"));
		testOpt("''", Arrays.asList(""));
		testOpt("\"\"", Arrays.asList(""));
	}

	@Test
	public void testInvalidOptionStrings() {
		testInvalidOpt("\\");
		testInvalidOpt("\"abcde");
		testInvalidOpt("'abcde");
	}

	@Test
	public void testWindowsBatchQuoting() {
		assertEquals("abc", quoteForBatchScript("abc"));
		assertEquals("\"a b c\"", quoteForBatchScript("a b c"));
		assertEquals("\"a \"\"b\"\" c\"", quoteForBatchScript("a \"b\" c"));
		assertEquals("\"a\"\"b\"\"c\"", quoteForBatchScript("a\"b\"c"));
		assertEquals("\"ab=\"\"cd\"\"\"", quoteForBatchScript("ab=\"cd\""));
		assertEquals("\"a,b,c\"", quoteForBatchScript("a,b,c"));
		assertEquals("\"a;b;c\"", quoteForBatchScript("a;b;c"));
		assertEquals("\"a,b,c\\\\\"", quoteForBatchScript("a,b,c\\"));
	}

	private void testOpt(String opts, List<String> expected) {
		assertEquals(String.format("解析参数失败：" + opts), expected, parseOptionString(opts));
	}

	private void testInvalidOpt(String opts) {
		try {
			parseOptionString(opts);
			fail("由于是非法字符，因此应该抛出异常");
		} catch (Exception e) {
			//pass
		}
	}
}
