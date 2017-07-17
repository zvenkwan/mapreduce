package autocomplete.java;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {

	public static void main(String[] args) {
//		public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		
//		String inputPath = "input";
//		String outputPath = "output";
//		String noGram = "2";
//		String threshold = "1";
//		String n = "1";
		String inputPath = args[0];
		String outputPath = args[1];
		String noGram = args[2];
		String threshold = args[3];
		String n = args[4];
		
		
		
		Configuration conf1 = new Configuration();
		conf1.set("textinputformat.record.delimiter", ".");
		conf1.set("noGram", noGram);
		
		//First Job 
	    Job job1;
		try {
			job1 = Job.getInstance(conf1);
			job1.setJobName("NGram");
			job1.setJarByClass(Driver.class);
			
			job1.setMapperClass(NGramBuilder.NGramMapper.class);
			job1.setReducerClass(NGramBuilder.NGramReducer.class);
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);
			TextInputFormat.setInputPaths(job1, new Path(inputPath));
			TextOutputFormat.setOutputPath(job1, new Path(outputPath));
			try {
				job1.waitForCompletion(true);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	    //Second Job 
	    
	    Properties prop = new Properties();
	    String driverClass = null;
	    String datbase = null;
	    String dbuser = null;
	    String dbpassword = null;
	    String dbconnector = null;
		try (InputStream input = new FileInputStream("config.properties")){
			prop.load(input);
			driverClass = prop.getProperty("driver");
			datbase = prop.getProperty("database");
			dbuser = prop.getProperty("dbuser");
			dbpassword = prop.getProperty("dbpassword");
			dbconnector = prop.getProperty("dbconnector");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	    Configuration conf2 = new Configuration();
	    conf2.set("threshold", threshold);
	    conf2.set("n", n);
	    DBConfiguration.configureDB(conf2,
	    		driverClass,
	    		datbase,
	    		dbuser,
	    		dbpassword);
	    Job job2;
		try {
			job2 = Job.getInstance(conf2);
		    job2.setJobName("LanguageModel");
		    job2.setJarByClass(Driver.class);
		    System.out.println("-----------------*-------------------");
		    System.out.println("-----------------*-------------------");
		    System.out.println("-----------------*-------------------");
		    System.out.println("-----------------*-------------------");
		    job2.addArchiveToClassPath(new Path("/mysql/" + dbconnector));
		    job2.setMapOutputKeyClass(Text.class);
		    job2.setMapOutputValueClass(Text.class);
		    job2.setOutputKeyClass(DBOutputWritable.class);
		    job2.setOutputValueClass(NullWritable.class);
		    job2.setMapperClass(LanguageModel.LanguageMapper.class);
		    job2.setReducerClass(LanguageModel.LanguageReducer.class);
		    job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(DBOutputFormat.class);
			DBOutputFormat.setOutput(
				     job2,
				     "ngram_language_model",    // output table name
				     new String[] { "starting_phrase", "following_word", "apprearance" }   //table columns
				     );
			TextInputFormat.setInputPaths(job2, new Path(outputPath));
			System.out.println("******");
			try {
				System.exit(job2.waitForCompletion(true)?0:1);
				System.out.println(4);
			} catch (Exception e) {
				System.out.println("in exception");
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("end");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
