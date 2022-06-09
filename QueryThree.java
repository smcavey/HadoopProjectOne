package queryThree;

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

public class QueryThree {
	
    public static class Map extends Mapper<Object, Text, Text, Text> {
    	//store CustomerID and Name as key and value
        private HashMap<Integer, String> custIdName = new HashMap<>();
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
                    String ageRange;
                    // while there's another record
                    while ((line = br.readLine()) != null) {
                    	// split up the customer record
                    	lineEles = line.split(",");
                        int age = Integer.parseInt(lineEles[2]);
                        // extract the age ranges
                        if(age>=10 && age<20){
                            ageRange = "[10,20)";
                        }else if(age<30){
                            ageRange = "[20,30)";
                        }else if(age<40){
                            ageRange = "[30,40)";
                        }else if(age<50){
                            ageRange = "[40,50)";
                        }else if(age<60){
                            ageRange = "[50,60)";
                        }else{
                            ageRange = "[60,70]";
                        }
                        // inset <CustomerID : age range, gender>
                        custIdName.put(Integer.parseInt(lineEles[0]), ageRange + "," + lineEles[3]);
                    }
                }
            }
        }

        // handle Transactions.csv
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // split up the Transactions.csv record
        	String[] transEles = value.toString().split(",");
            //output CustomerID and name, total
            context.write(new Text(custIdName.get(Integer.parseInt(transEles[1]))), new Text(transEles[2] +","+ transEles[1]));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<Integer, Float> custIDSum = new HashMap<>();
            float minTotal = Integer.MAX_VALUE;
            float maxTotal = Integer.MIN_VALUE;
            float sumTransactions = 0;
            float totalSumTransactions = 0;
            for (Text val : values) {
                String line = val.toString();
                String[] lineEles = line.split(",");
                int custID = Integer.parseInt(lineEles[1]);
                if (custIDSum.containsKey(custID)) {
                	// increase transactions
                    sumTransactions = custIDSum.get(custID) + Float.parseFloat(lineEles[0]);
                }
                else {
                	// else init transactions
                    sumTransactions = Float.parseFloat(lineEles[0]);
                }
                // update running sum of transactions
                totalSumTransactions += Float.parseFloat(lineEles[0]);
                if (maxTotal < sumTransactions) {
                    maxTotal = sumTransactions;
                }
                custIDSum.put(custID, sumTransactions);
            }
            float avg = totalSumTransactions/custIDSum.size();
            minTotal = Collections.min(custIDSum.values());
            // set the output of min total, max total, and average
            result.set(minTotal + "," + maxTotal + "," + avg);
            // output the age range, gender + above values
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	Job job = Job.getInstance(new Configuration());
        job.setJarByClass(QueryThree.class);
        job.setJobName("query three");
        job.setMapperClass(QueryThree.Map.class);
        job.setReducerClass(QueryThree.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // /user/ds503/input/Customers.csv for replicated join because Customers.csv is small
        job.addCacheFile(new Path(args[0]).toUri());
        // /user/ds503/input/Transactions.csv
        FileInputFormat.addInputPath(job, new Path(args[1]));
        // /user/ds503/output/p1q3
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}