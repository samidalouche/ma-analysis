package org.hackreduce.test;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;


public class SmaTest {

	@Test
	@Ignore
	public void shouldReturnSMAsForArray() {
		double[] in = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
		double[] out = new double[in.length];
		
		MInteger begIdx = new MInteger();
		MInteger outNbElement = new MInteger();
		new Core().sma(0, in.length-1, in, 2, begIdx, outNbElement, out);
		
		Assert.assertEquals(Arrays.asList(1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5), Arrays.asList(out));
	}
}
