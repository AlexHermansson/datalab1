import java.nio.file.*;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.Text;

public class Test {
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

    public static String readFileAsString(String fileName) throws Exception { 
	    String data = ""; 
	    data = new String(Files.readAllBytes(Paths.get(fileName))); 
	    return data; 
  	}

    public static void main(String[] args) throws Exception {

    	Text tempId = new Text();

    	TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
    	// File file = new File("/Users/alexhermansson/Documents/KTH/DataIntensive/lab1/src/topten/data/users.xml");
    	String inputString = readFileAsString("/Users/alexhermansson/Documents/KTH/DataIntensive/lab1/src/topten/data/test.xml");
    	Map<String, String> ourMap = transformXmlToMap(inputString);

    	String id = ourMap.get("Id");
    	System.out.println("Id: " + id);
    	tempId.set(id);

    	Integer rep = Integer.parseInt(ourMap.get("Reputation"));
    	System.out.println("Reputation: " + rep);

    	repToRecordMap.put(rep, tempId);
    	System.out.println("reputation map: " + repToRecordMap);

    }
}
