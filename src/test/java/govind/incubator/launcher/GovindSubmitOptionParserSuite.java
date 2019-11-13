package govind.incubator.launcher;

import govind.incubator.launcher.util.GovindSubmitOptionParser;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-12
 */
public class GovindSubmitOptionParserSuite extends BaseSuite {

	private GovindSubmitOptionParser parser;

	@Before
	public void setup() {
		parser = spy(new DummyParser());
	}

	@Test
	public void testAllOptions() {
		int count = 0;
		for (String[] optNames : parser.opts) {
			for (String name : optNames) {
				String value = name + "-value;";
				parser.parse(Arrays.asList(name, value));
				count++;
				verify(parser).handle(eq(optNames[0]), eq(value));
				verify(parser, times(count)).handle(any(), any());
				verify(parser, times(count)).handleExtraArgs(eq(Collections.emptyList()));
			}
		}

		int unknownCnt =  0;
		for (String[] aSwitch : parser.switches) {
			for (int i = 0; i < aSwitch.length; i++) {
				String value = aSwitch[i] + "-unused-value";
				parser.parse(Arrays.asList(aSwitch[i], value));
				count++;unknownCnt++;
				verify(parser, times(i + 1)).handle(eq(aSwitch[0]), isNull());
				verify(parser, times(count)).handle(any(), any());
				verify(parser, times(unknownCnt)).handleUnknown(any());
				verify(parser, times(count)).handleExtraArgs(eq(Collections.emptyList()));
			}
		}
	}


	@Test
	public void testExtraOptions() {
		List<String> args  = Arrays.asList(parser.MASTER, parser.MASTER, "foo", "bar");
		parser.parse(args);

		verify(parser).handle(eq(parser.MASTER), eq(parser.MASTER));
		verify(parser).handleUnknown(eq("foo"));
		verify(parser).handleExtraArgs(eq(Arrays.asList("bar")));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMissingArg() {
		parser.parse(Arrays.asList(parser.MASTER));
	}

	@Test
	public void testEqualSeparatedOption() {
		List<String> arg = Arrays.asList(parser.MASTER + "=" + parser.MASTER);
		parser.parse(arg);
		verify(parser).handle(eq(parser.MASTER), eq(parser.MASTER));
		verify(parser).handleExtraArgs(eq(Collections.emptyList()));
	}

	private static class DummyParser extends GovindSubmitOptionParser {
		@Override
		public boolean handle(String opt, String value) {
			return true;
		}

		@Override
		public boolean handleUnknown(String opt) {
			return false;
		}

		@Override
		public void handleExtraArgs(List<String> extra) {
		}
	}
}
