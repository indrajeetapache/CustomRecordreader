package customrecorddriver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class customrecorddriver extends Configured implements Tool {

	public static class customrecordmap extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(new IntWritable(1), value);
		}
	}

	public static class customrecordeduce extends
			Reducer<IntWritable, Text, Text, Text> {
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) {

		}
	}

	public static void main(String[] args) throws Exception  {
		// TODO Auto-generated method stub
		System.exit(ToolRunner.run(new Configuration(), new customrecorddriver(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"customrecorddriver");
		job.setJarByClass(customrecorddriver.class);
		job.setInputFormatClass(CustomFileInputFormat.class);
		
		
		CustomFileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		
		job.setMapperClass(customrecordmap.class);
		job.setReducerClass(customrecordeduce.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
	    return job.waitForCompletion(true) ? 0 : 1;
	}

}
