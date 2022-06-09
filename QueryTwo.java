package queryTwo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;

public class QueryTwo {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        // store CustomerID and CountryCode as key and value
        private HashMap<Integer, Integer> custCountry = new HashMap<>();
        // read the first input file Customers.csv
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
        	// get cached Customers.csv split
            URI[] inpCSV = context.getCacheFiles();
            // confirm there's records in it
            if (inpCSV != null && inpCSV.length > 0) {
            	// for each file
                for (URI file : inpCSV) {
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path path = new Path(file.toString());
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    String line;
                    String[] lineEles;
                    // while there's another record
                    while ((line = br.readLine()) != null) {
                    	// split up the customer record
                        lineEles = line.split(",");
                        // insert <CustomerID : CountryCode>
                        custCountry.put(Integer.parseInt(lineEles[0]), Integer.parseInt(lineEles[4]));
                    }
                }
            }
        }

        // handle Transactions.csv
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	// split up the the Transactions.csv record
            String[] transEles = value.toString().split(",");
            // output CountryCode and CustomerID, TransTotal
            context.write(new Text(custCountry.get(Integer.parseInt(transEles[1])).toString()), new Text(transEles[1] + "," + transEles[2]));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<Integer, Float> countryCodeCustNum = new HashMap<>();
            float minTotal = Integer.MAX_VALUE;
            float maxTotal = Integer.MIN_VALUE;
            float sumTransactions;
            // loop over key and values from the mapper
            for (Text val : values) {
            	String line = val.toString();
                String[] lineEles = line.split(",");
                int custID = Integer.parseInt(lineEles[0]);
                if (countryCodeCustNum.containsKey(custID)) {
                	// increment transactions total if CustomerID already in hash map
                    sumTransactions = countryCodeCustNum.get(custID) + Float.parseFloat(lineEles[1]);
                }
                else {
                	// else init the transactions total
                    sumTransactions = Float.parseFloat(lineEles[1]);
                }
                if(sumTransactions>maxTotal){
                	// set max total
                    maxTotal = sumTransactions;
                }
                // insert the CustomerID and sum of transactions into the hash map
                countryCodeCustNum.put(custID, sumTransactions);
            }
            minTotal = Collections.min(countryCodeCustNum.values());
            // set the output of number of customers, minimum total, maximum total
            result.set(countryCodeCustNum.size() + "," + minTotal + "," + maxTotal);
            // output country code + above values
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	Job job = Job.getInstance(new Configuration());
        job.setJarByClass(QueryTwo.class);
        job.setJobName("query two");
        job.setMapperClass(QueryTwo.Map.class);
        job.setReducerClass(QueryTwo.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // /user/ds503/input/Customers.csv for replicated join because Customers.csv is small
        job.addCacheFile(new Path(args[0]).toUri());
        // /user/ds503/input/Transactions.csv
        FileInputFormat.addInputPath(job, new Path(args[1]));
        // /user/ds503/output/p1q2
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}