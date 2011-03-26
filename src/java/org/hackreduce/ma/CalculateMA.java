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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.examples.stockexchange.RecordCounter.Count;
import org.hackreduce.mappers.StockExchangeMapper;
import org.hackreduce.models.StockExchangeRecord;

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
		TOTAL_KEYS,
		UNIQUE_SYMBOLS
	}

	public static class MACalculatorMapper extends StockExchangeMapper<Text, Text> {

		@Override
		protected void map(StockExchangeRecord record, Context context) throws IOException, InterruptedException {
			context.getCounter(Count.TOTAL_KEYS).increment(1);
			context.write(new Text(record.getStockSymbol()), new Text(sdf.format(record.getDate()) + "," + record.getStockPriceClose()));
		}

	}
	
	public static class MACalculatorReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.getCounter(Count.UNIQUE_SYMBOLS).increment(1);
			
			List<ClosingValue> closingValues = closingValuesSortedByDate(values);
			
			List<AverageOnDate> sma20Values = calculateSMA(20, closingValues);
			List<AverageOnDate> sma100Values = calculateSMA(100, closingValues);
			
			VariationTracker variationTracker = new VariationTracker();
			
			for(int i = 0; i < sma100Values.size() ; i++) {
				AverageOnDate v = sma100Values.get(i);
				
				Double sma20 = sma20Values.get(i + 80).getAverage();
				Double sma100 = sma100Values.get(i).getAverage();

				Variation variation = variationTracker.nextVariation(sma20, sma100);
				
				writeOutput(key, context, v, sma20, sma100, variation);
				
			}

		}

		private void writeOutput(Text key, Context context, AverageOnDate v, Double sma20, Double sma100, Variation variation)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			sb.append(sdf.format(v.getDate()))
				.append(",")
				.append(v.getClosePrice())
				.append(",")
				.append(sma20)
				.append(",")
				.append(sma100)
				.append(",")
				.append(variation.getOutput());
			
			context.write(key, new Text(sb.toString()));
		}

		private List<ClosingValue> closingValuesSortedByDate(Iterable<Text> values) {
			
			List<ClosingValue> temp = new ArrayList<ClosingValue>();
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
				
				temp.add(new ClosingValue(date, closePrice));
			}
			Collections.sort(temp);
			return temp;
		}

		private List<AverageOnDate> calculateSMA(int numberOfDaysWindow, List<ClosingValue> closingValues) {
			double [] prices = new double[closingValues.size()];
			int i = 0;
			for(ClosingValue c : closingValues) {
				prices[i] = c.getValue();
				i++;
			}
			
			double[] out = new double[prices.length];
			
			MInteger begIdx = new MInteger();
			MInteger outNbElement = new MInteger();
			new Core().sma(0, prices.length-1, prices, numberOfDaysWindow, begIdx, outNbElement, out);
			
			List<AverageOnDate> result = new ArrayList<AverageOnDate>();
			for(int x = 0 ; x < outNbElement.value ; x++) {
				ClosingValue v = closingValues.get(begIdx.value + x);
				Date d = v.getDate();
				result.add(new AverageOnDate(d, out[x], v.getValue()));				
			}

			return result;
		}
	}

	
	public static class SMACrossOverPointsReducer extends Reducer<Text, Text, Text, Text> {

		


		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			
			for(Text v : values) {
				String input = v.toString();
				String[] attributes = v.toString().split(",");
				
				if (attributes.length != 3)
					throw new IllegalArgumentException("Input string given did not have 2 values in CSV format");

				Date date;
				try {
					date = sdf.parse(attributes[0]);
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
				
			}
		}
	}

	private static class AverageOnDate {
		private Date date;
		private Double average;
		private Double closePrice;
		
		public AverageOnDate(Date date, Double average, Double closePrice) {
			super();
			this.date = date;
			this.average = average;
			this.closePrice = closePrice;
		}
		public Date getDate() {
			return date;
		}
		public Double getAverage() {
			return average;
		}
		public Double getClosePrice() {
			return closePrice;
		}

	}
	
	private static class ClosingValue implements Comparable<ClosingValue>{
		private Date date;
		private Double value;
		public ClosingValue(Date date, Double value) {
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
		public int compareTo(ClosingValue o) {
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

		Job job = generateSMAJob(args, conf);

		if(!job.waitForCompletion(true)) return 1;
		
		return 0;
		
	}

	private Job generateSMAJob(String[] args, Configuration conf) throws IOException {
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
		Path output = new Path(args[1] + "-sma");
		FileSystem.get(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		return job;
	}
	
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new CalculateMA(), args);
		System.exit(result);
	}
	
	private enum Variation {
		UP("1"), DOWN("-1"), STABLE("0");
		
		private String output;

		private Variation(String output) {
			this.output = output;
		}

		public String getOutput() {
			return output;
		}

	}
	
	private  enum State {
		SMA20_HIGHER, SMA100_HIGHER, EQUAL
	}
	
	private static class VariationTracker {
		private State previousState = null;
		private State currentState = null;
		
		public Variation nextVariation(Double sma20, Double sma100) {
			
			if(sma20 > sma100) {
				currentState = State.SMA20_HIGHER;
			} else if(sma100 < sma20) {
				currentState = State.SMA100_HIGHER;
			} else {
				currentState = State.EQUAL;
			}
			
			Variation variation = Variation.STABLE;
			if(previousState != null) {
				if(! previousState.equals(currentState)) {
					if(previousState.equals(State.SMA100_HIGHER)) {
						variation = Variation.UP;
					} else {
						variation = Variation.DOWN;
					}
				} else {
					// stable
				}
			}
			previousState = currentState;
			return variation;
		}
	}

}
