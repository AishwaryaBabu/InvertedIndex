package edu.rutgers.cloud;

import java.io.IOException;

import java.io.StringReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class Main{

	public static class SearchReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterable<Text> values, Context ctx) 
				throws IOException, InterruptedException {
			HashMap<String, Integer> index=new HashMap<String, Integer>();
			
			for (Text file: values){
				if(index.containsKey(file.toString())){
					index.put(file.toString(), index.get(file.toString())+1);
				}
				else{
					index.put(file.toString(),new Integer(1));
				}
			}
			ctx.write(key, new Text(index.toString()));

		}
	}

	public static class SearchMapper  extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {

			//StandardAnalyzer stopWords=new StandardAnalyzer(Version.LUCENE_33);
			EnglishAnalyzer stopWords = new EnglishAnalyzer(Version.LUCENE_36);
			TokenStream analyzedTokens=stopWords.tokenStream(null, new StringReader(value.toString().toLowerCase()));
			//StringTokenizer tokenizer = new StringTokenizer(value.toString());
			
			//TokenStream analyzedTokens=new PorterStemFilter(tokens);
			
			FileSplit fileSplit = (FileSplit)ctx.getInputSplit();
			Text filePath = new Text(fileSplit.getPath().getName());
			CharTermAttribute cattr = analyzedTokens.addAttribute(CharTermAttribute.class);
			analyzedTokens.reset();
			while (analyzedTokens.incrementToken()) {
				ctx.write(new Text(cattr.toString()), filePath);
			}
			analyzedTokens.end();
			analyzedTokens.close();
			stopWords.close();

		}
	}

	public static void main(String[] args) throws Exception {
		
		if(args.length <2){
			throw new Exception("Usage <input directory> <output directory>");
		}
		Configuration conf = new Configuration();

		Job job = new Job(conf);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(Main.SearchMapper.class);
		job.setCombinerClass(Main.SearchReducer.class);
		job.setReducerClass(Main.SearchReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(Main.class);
		job.waitForCompletion(true);
	}
}

