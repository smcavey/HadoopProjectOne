package queryOne;

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
import java.util.HashMap;

public class QueryOne {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        // store CustomerID and Name as key and value
        private HashMap<Integer, String> custIdNameSalary = new HashMap<>();
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
                    	// insert <CustomerID : Name,Salary
                        custIdNameSalary.put(Integer.parseInt(lineEles[0]), lineEles[1] + "," + lineEles[5]);
                    }
                }
            }
        }

        // handle Transactions.csv
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // split up the Transactions.csv record
        	String[] transEles = value.toString().split(",");
            // output CustomerID and Name, Salary, TransTotal
            context.write(new Text(transEles[1]), new Text(custIdNameSalary.get(Integer.parseInt(transEles[1])) + "," + transEles[2] + "," + transEles[3]));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = "";
            float salary = 0;
            int numTransactions = 0;
            float totalSum = 0;
            int minItems = Integer.MAX_VALUE;
            // loops over key and values from the mapper
            for (Text val : values) {
                String line = val.toString();
                String[] lineEles = line.split(",");
                name = lineEles[0];
                salary = Float.parseFloat(lineEles[1]);
                numTransactions += 1;
                totalSum += Float.parseFloat(lineEles[2]);
                if (minItems > Integer.parseInt(lineEles[3])) {
                    minItems = Integer.parseInt(lineEles[3]);
                }
            }
            // set the output of name, salary, number of transactions, total sum, and min items
            result.set(name + ',' + salary + ',' + numTransactions + ',' + totalSum + ',' + minItems);
            // output CustomerID + the above values
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	Job job = Job.getInstance(new Configuration());
        job.setJarByClass(QueryOne.class);
        job.setJobName("query one");
        job.setMapperClass(QueryOne.Map.class);
        job.setReducerClass(QueryOne.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // /user/ds503/input/Customers.csv for replicated join because Customers.csv is small
        job.addCacheFile(new Path(args[0]).toUri());
        // /user/ds503/input/Transactions.csv
        FileInputFormat.addInputPath(job, new Path(args[1]));
        // /user/ds503/output/p1q1
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}