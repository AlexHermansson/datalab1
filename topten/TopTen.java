package topten;

import java.nio.file.*;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
		    String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
		    for (int i = 0; i < tokens.length - 1; i += 2) {
			String key = tokens[i].trim();
			String val = tokens[i + 1];
			map.put(key.substring(0, key.length() - 1), val);
		    }
		} catch (StringIndexOutOfBoundsException e) {
		    System.err.println(xml);
		}

		return map;
    }

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
	// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		// value comes as a line from the input file
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			try {

			    Map<String, String> theMap = transformXmlToMap(value.toString());
			    String id = theMap.get("Id");

			    if (id != null) {
				    String rep = theMap.get("Reputation");
				    repToRecordMap.put(new Integer(rep), new Text(id));
			    }
		    }

		    catch (Exception e) {
		    	e.printStackTrace();
		    }

		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
		    // Output our ten records to the reducers with a null key

		    try {

			    //NullWritable nW = new NullWritable();
			    for (int i = 0; i < 10; i++) {
			    	String rep = repToRecordMap.pollLastEntry().getKey().toString();
			    	String id = repToRecordMap.pollLastEntry().getValue().toString();
			    	Text idRep = new Text(id + " " + rep);

			    	context.write(null, idRep);
			    }
		    }

		    catch (Exception e) {
		    	e.printStackTrace();
		    }
		}
    }


    public static class TopTenReducer extends Reducer<NullWritable, Text, Text, Text> {
    //public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
	// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		    //<FILL IN>

			try {

				for (Text idRep : values) {
					String id = idRep.toString().split(" ")[0];
					String rep = idRep.toString().split(" ")[1];
					repToRecordMap.put(new Integer(rep), new Text(id));
				}

				for (int i = 1; i < 11; i++) {
			    	String rep = repToRecordMap.pollLastEntry().getKey().toString();
			    	String id = repToRecordMap.pollLastEntry().getValue().toString();

			    	// Put stuff into the table
			    	/*
			    	Put insHBase = new Put(Bytes.toBytes(i));
			    	insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id));
			    	insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(rep));
			    	context.write(null, insHBase);
			    	*/

			    	context.write(new Text(rep), new Text(id));

			    }
			    
		    } 

		    catch (Exception e) {
		    	e.printStackTrace();
		    }
		}
    }

    public static void main(String[] args) throws Exception {
		//<FILL IN>
    	//Configuration conf = HBaseConfiguration.create();
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf);

    	job.setMapperClass(TopTenMapper.class);
    	job.setCombinerClass(TopTenReducer.class);
    	job.setReducerClass(TopTenReducer.class);

    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);

    	//job.setJarByClass(TopTen.class);
    	job.setNumReduceTasks(1);

    	//Define output table
    	//TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

    	FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
