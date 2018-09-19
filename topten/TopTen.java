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

		    	System.out.println("Number of elements in cleanup: " + repToRecordMap.size());

			    //NullWritable nW = new NullWritable();
			    for (int i = 0; i < 10; i++) {

			    	Map.Entry<Integer, Text> lastEntry = repToRecordMap.pollLastEntry();
			    	String rep = lastEntry.getKey().toString();
			    	String id = lastEntry.getValue().toString();
			    	Text idRep = new Text(id + " " + rep);

			    	System.out.println("idrep: " + idRep);
			    	System.out.println("Number of elements in tree after: " + repToRecordMap.size());


			    	context.write(NullWritable.get(), idRep);
			    	
			    }
		    }

		    catch (Exception e) {
		    	e.printStackTrace();
		    }
		}
    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
	// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			try {

				System.out.println("in reduce");

				for (Text idRep : values) {
					String id = idRep.toString().split(" ")[0];
					String rep = idRep.toString().split(" ")[1];
					repToRecordMap.put(new Integer(rep), new Text(id));
				}

				System.out.println("Elements in tree in reducer after: " + repToRecordMap.size());

				for (int i = 1; i < 11; i++) {
					Map.Entry<Integer, Text> lastEntry = repToRecordMap.pollLastEntry();
			    	String rep = lastEntry.getKey().toString();
			    	String id = lastEntry.getValue().toString();

			    	// Put stuff into the table
			    	Put insHBase = new Put(Bytes.toBytes(i));
			    	insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id));
			    	insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(rep));
			    	context.write(null, insHBase);

			    }

			    System.out.println("Done with reduce");
			    
		    } 

		    catch (Exception e) {
		    	e.printStackTrace();
		    }
		}
    }

    public static void main(String[] args) throws Exception {
    	Configuration conf = HBaseConfiguration.create();
    	Job job = Job.getInstance(conf);

    	job.setMapperClass(TopTenMapper.class);
    	job.setJarByClass(TopTen.class);
    	job.setNumReduceTasks(1);

    	job.setMapOutputKeyClass(NullWritable.class);
    	job.setMapOutputValueClass(Text.class);


    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
