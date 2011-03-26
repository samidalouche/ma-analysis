package org.hackreduce.ma;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.StockExchangeMapper;
import org.hackreduce.models.StockExchangeRecord;

import sun.misc.Compare;

import com.tictactec.ta.lib.Core;
import com.tictactec.ta.lib.MInteger;


/**
 * This MapReduce job will read the NASDAQ or NYSE daily prices dataset and output the highest market caps
 * obtained by each Stock symbol.
 *
 */
public class CalculateMA extends Configured implements Tool {
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public enum Count {

	}

	public static class MACalculatorMapper extends StockExchangeMapper<Text, Text> {

		@Override
		protected void map(StockExchangeRecord record, Context context) throws IOException, InterruptedException {
			context.write(new Text(record.getStockSymbol()), new Text(sdf.format(record.getDate()) + "," + record.getStockPriceClose()));
		}

	}

	public static class MACalculatorReducer extends Reducer<Text, Text, Text, Text> {

		NumberFormat currencyFormat = NumberFormat.getCurrencyInstance(Locale.getDefault());

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<ClosingValueOnDate> closingValues = closingValues(values);
			Collections.sort(closingValues);
			List<AverageOnDate> average20 = calculateSMA(20, closingValues);
			List<AverageOnDate> averages100 = calculateSMA(100, closingValues);
			
			int i = 0;
			for(AverageOnDate v : averages100) {
				StringBuilder sb = new StringBuilder();
				sb.append(sdf.format(v.getDate()))
				.append("/")
				.append(average20.get(i + 80).getAverage()) // 100-20 = 80
				.append("/")
				.append(averages100.get(i).getAverage());

				context.write(key, new Text(sb.toString()));
				i++;
			}

		}

		private List<ClosingValueOnDate> closingValues(Iterable<Text> values) {
			
			List<ClosingValueOnDate> temp = new ArrayList<ClosingValueOnDate>();
			for(Text v : values) {
				String[] attributes = v.toString().split(",");
				if (attributes.length != 2)
					throw new IllegalArgumentException("Input string given did not have 2 values in CSV format");

				Date date;
				try {
					date = sdf.parse(attributes[0]);
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
				Double closePrice = Double.parseDouble(attributes[1]);
				
				temp.add(new ClosingValueOnDate(date, closePrice));
			}
			return temp;
		}

		private List<AverageOnDate> calculateSMA(int numberOfDaysWindow, List<ClosingValueOnDate> closingValues) {
			double [] prices = new double[closingValues.size()];
			int i = 0;
			for(ClosingValueOnDate c : closingValues) {
				prices[i] = c.getValue();
				i++;
			}
			
			double[] out = new double[prices.length];
			
			MInteger begIdx = new MInteger();
			MInteger outNbElement = new MInteger();
			new Core().sma(0, prices.length-1, prices, numberOfDaysWindow, begIdx, outNbElement, out);
			
			List<AverageOnDate> result = new ArrayList<AverageOnDate>();
			for(int x = 0 ; x < outNbElement.value ; x++) {
				Date d = closingValues.get(begIdx.value + x).getDate();
				result.add(new AverageOnDate(d, out[x]));				
			}

			return result;
		}
	}

	private static class AverageOnDate {
		private Date date;
		private Double average;
		public AverageOnDate(Date date, Double average) {
			super();
			this.date = date;
			this.average = average;
		}
		public Date getDate() {
			return date;
		}
		public Double getAverage() {
			return average;
		}

	}
	
	private static class ClosingValueOnDate implements Comparable<ClosingValueOnDate>{
		private Date date;
		private Double value;
		public ClosingValueOnDate(Date date, Double value) {
			super();
			this.date = date;
			this.value = value;
		}
		public Date getDate() {
			return date;
		}
		public Double getValue() {
			return value;
		}
		
		@Override
		public int compareTo(ClosingValueOnDate o) {
			return date.compareTo(o.getDate());
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		if (args.length != 2) {
			System.err.println("Usage: " + getClass().getName() + " <input> <output>");
			System.exit(2);
		}

		// Creating the MapReduce job (configuration) object
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setJobName(getClass().getName());

		// Tell the job which Mapper and Reducer to use (classes defined above)
		job.setMapperClass(MACalculatorMapper.class);
		job.setReducerClass(MACalculatorReducer.class);

		// Configure the job to accept the NASDAQ/NYSE data as input
		StockExchangeMapper.configureJob(job);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
		Path output = new Path(args[1]);
		FileSystem.get(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new CalculateMA(), args);
		System.exit(result);
	}

}