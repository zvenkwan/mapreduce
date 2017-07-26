package pagerank.java;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
        	String line[] = value.toString().trim().split("/t");
        	String keyString = line[0];
        	if(line.length < 2 || line[1].trim().equals("")) {
        		return;
//        		should throw exceptions
        	}
        	String tos[] = line[1].split(",");
        	for(String to: tos) {
        		context.write(new Text(keyString), new Text(to + "=" + (double)1/tos.length));
        	}
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
        	String line[] = value.toString().trim().split("\t");
        	context.write(new Text(line[0]), new Text(line[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

    	float beta;
    	
    	@Override
    	public void setup(Context context) {
    		Configuration conf = context.getConfiguration();
    		beta = conf.getFloat("beta", 0.2f);
    	}

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
        	double pr = 0;
        	List<String> transitionCells = new ArrayList<String>();
        	
        	for(Text val : values) {
        		if(val.toString().contains("=")) {
        			transitionCells.add(val.toString());
        		}
        		else {
        			pr = Double.parseDouble(val.toString());
        		}
        	}
        	
        	for(String cell : transitionCells) {
        		String[] pair = cell.split("=");
        		context.write(new Text(pair[0]), new Text(String.valueOf(beta*pr*Double.parseDouble(pair[1]))));
        	}
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("beta", Float.parseFloat(args[3]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        
        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
