package autocomplete.java;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramBuilder {

	public static class NGramMapper extends Mapper<Object, Text, Text, IntWritable> {

        private int noGram;

        @Override
        public void setup(Context context) throws IOException{
        	Configuration config = context.getConfiguration();

    		noGram = config.getInt("noGram", 4);
    		context.getCounter("********************", "no of gram is " + noGram);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String sentence = value.toString().trim().toLowerCase();
        	String[] words = sentence.split("\\s+");
        	for(int i = 0; i < words.length - 1; i ++) {
        		StringBuilder sb = new StringBuilder();
        		sb.append(words[i]);
        		for(int j = 1; i + j < words.length && j < noGram; j++) {
        			sb.append(" ");
        			sb.append(words[i+j]);
        			context.write(new Text(sb.toString()), new IntWritable(1));
        		}
        	}
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
        	int sum = 0;
        	for(IntWritable v : values) {
        		sum += v.get();
        	}
        	context.write(key, new IntWritable(sum));
        }
    }
}
