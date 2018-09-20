package topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;


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
		} 
		catch (StringIndexOutOfBoundsException e) {
		    System.err.println(xml);
		}
		return map;
    }

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		// value comes as a line from the input file (one user).
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		    Map<String, String> theMap = transformXmlToMap(value.toString());
		    String id = theMap.get("Id");
		    String rep = theMap.get("Reputation");
		    boolean noNulls = ((id != null) && (rep != null));

		    if (noNulls) {
		    	Integer intRep = new Integer(rep);
		    	if (repToRecordMap.containsKey(intRep)) {
			    	String existingId =  repToRecordMap.get(intRep).toString();
			    	id += " " + existingId;
			    }
			    repToRecordMap.put(intRep, new Text(id));
		    }
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
		    // Output our ten records to the reducers with a null key
		    for (int i = 0; i < 10; i++) {
		    	Map.Entry<Integer, Text> lastEntry = repToRecordMap.pollLastEntry();
		    	String rep = lastEntry.getKey().toString();
		    	String id = lastEntry.getValue().toString();
		    	Text idRep = new Text(id + " " + rep);

		    	context.write(NullWritable.get(), idRep);
		    }
		}
    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			putIdsAndReputationInTree(values);
			putTopTenInTable(context);
		}

		private void putIdsAndReputationInTree(Iterable<Text> values) {
			for (Text idRep : values) {

				StringTokenizer itr = new StringTokenizer(idRep.toString());
				String ids = itr.nextToken();
				while (itr.countTokens() > 1) {
					ids += ", " + itr.nextToken();
				}

				String rep = itr.nextToken();
				repToRecordMap.put(new Integer(rep), new Text(ids));
			}
		}

		private void putTopTenInTable(Context context) throws IOException, InterruptedException {
			for (int i = 1; i < 11; i++) {
				Map.Entry<Integer, Text> lastEntry = repToRecordMap.pollLastEntry();
		    	String rep = lastEntry.getKey().toString();
		    	String id = lastEntry.getValue().toString();

		    	Put insHBase = new Put(Bytes.toBytes(i));
		    	insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id));
		    	insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(rep));
		    	context.write(null, insHBase);
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
    }
}
