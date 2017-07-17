package autocomplete.java;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class LanguageModel {

	public static class LanguageMapper extends Mapper<Object, Text, Text, Text> {

		private int threshold;
        @Override
        public void setup(Context context) throws IOException{
        	Configuration config = context.getConfiguration();
        	threshold = config.getInt("threshold", 10);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String line = value.toString().trim();
        	String[] words_count = line.split("\t");
        	if(words_count.length < 2) return;
        	int count = Integer.parseInt(words_count[1]);
        	
        	if(count < threshold) return;
        	
        	String[] words = words_count[0].split("\\s+");
        	StringBuilder sb = new StringBuilder();
        	for(int i = 0; i < words.length - 1; i ++) {
        		sb.append(words[i]).append(" ");
        	}
        	String outputKey = sb.toString().trim();
        	String outputValue = words[words.length - 1] + "=" + count;
        	context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class LanguageReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

    	int n;

    	// get the n parameter from the configuration
        @Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
			for (Text val : values) {
				String cur_val = val.toString().trim();
				String word = cur_val.split("=")[0].trim();
				int count = Integer.parseInt(cur_val.split("=")[1].trim());
				if(tm.containsKey(count)) {
					tm.get(count).add(word);
				}
				else {
					List<String> list = new ArrayList<>();
					list.add(word);
					tm.put(count, list);
				}
			}

			Iterator<Integer> iter = tm.keySet().iterator();
			
			for(int j=0 ; iter.hasNext() && j < n; j++) {
				int keyCount = iter.next();
				List<String> words = tm.get(keyCount);
				for(String curWord: words) {
					context.write(new DBOutputWritable(key.toString(), curWord, keyCount), NullWritable.get());
					j++;
				}
			}
        }
    }
}
