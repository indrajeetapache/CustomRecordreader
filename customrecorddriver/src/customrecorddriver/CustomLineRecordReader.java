package customrecorddriver;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class CustomLineRecordReader extends RecordReader<LongWritable, Text> {

	
	private LineRecordReader in;
	private Text lineValue;
	private LongWritable key = new LongWritable();
	private Text value = new Text();

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		// This InputSplit is a FileInputSplit
		// FileSplit split = (FileSplit) genericSplit;

		// Retrieve configuration, and Max allowed
		// bytes for a single record
		// Configuration job = context.getConfiguration();
		// this.maxLineLength=job.getInt(
		// "mapred.linerecordreader.maxlength",
		// Integer.MAX_VALUE);

		// Split "S" is responsible for all records
		// starting from "start" and "end" positions
		// start = split.getStart();
		// end = start + split.getLength();

		in = new LineRecordReader();
		in.initialize(genericSplit, context);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		boolean appended, isNextLineAvailable;
		boolean retval;
		byte space[] = { ' ' };
		value.clear();
		isNextLineAvailable = false;
		do {
			appended = false;
			retval = in.nextKeyValue();
			
			if (retval) {
				lineValue = in.getCurrentValue();
				if (lineValue.toString().length() > 0) 
				{
					byte[] rawline = lineValue.getBytes();
					int rawlinelen = lineValue.getLength();
					value.append(rawline, 0, rawlinelen);
					value.append(space, 0, 1);
					appended = true;
				}
				isNextLineAvailable = true;
			}
		} while (appended);

		return isNextLineAvailable;

	}

}
