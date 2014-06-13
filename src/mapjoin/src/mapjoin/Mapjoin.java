/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mapjoin;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author Thinkpad
 */
import java.io.*;
import java.math.BigInteger;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.TableLine;

public class Mapjoin {

    static final BigInteger one = new BigInteger("1");
    static Integer r;
    static Integer F1;
    static Integer D1;
    static ArrayList<Integer> tablesizelist = new ArrayList<Integer>();
    static ArrayList<Integer> tablebucketsizelist;
    static Integer F1segments;
    static Integer D1segments;
    static Integer bucketsize;
    static Integer F1sidelength;
    static Integer D1sidelength;

    public static class JoinMap
            extends Mapper<Object, Text, Text, Text> {

        //private final static IntWritable one = new IntWritable(1);
        private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(JoinMap.class);
        private TableLine tableline;
        private Text value = new Text();
        private Configuration cache;
        private Integer ht = new Integer("100000");
        BigInteger sum = new BigInteger("0");
        boolean first = true;
        Integer F1sidelength;
        Integer D1sidelength;
        Integer F1segments;
        Integer D1segments;
        Integer tablesum;
        Integer R1;
        Random r = new Random();
        String table2;
        Map<String,ArrayList> tables = new HashMap<String,ArrayList>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            cache = context.getConfiguration();
//            tablesum = cache.getInt("tablesum", 0);
//            R1 = cache.getInt("R1", 0);
            table2 = cache.get("table2");
            cachetable(table2);
            LOG.info("map setup");

        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            tables.clear();
            tables = null;
            context.write(new Text("sum"), new Text(sum.toString()));
        }

        private void cachetable(String table) throws IOException {
            Path path = new Path(table);
            FSDataInputStream input = FileSystem.get(cache).open(path, 1024);
            BufferedReader br = new BufferedReader(new InputStreamReader(input));
            String line;
//            ArrayList<String> row = new ArrayList<String>();
            while ((line = br.readLine()) != null) {
                String joint = line.split("\\|")[0];
                if(tables.get(joint) == null){
                    ArrayList<String> values = new ArrayList<String>();
                    values.add(line);
                    tables.put(joint, values);
                }else{
                    tables.get(joint).add(line);
                }
//                row.add(line);
            }
//            tables.put(row);
        }
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String joint = value.toString().split("\\|")[0];
            ArrayList<String> t2 = tables.get(joint);
            if(t2 == null){
                return;
            }else{
                for(String s:t2){
//                    String outvalue = value.toString() + " " + s;
//                    context.write(new Text(joint), new Text(outvalue));
                    sum = sum.add(one);
                }
                t2.clear();
                t2 = null;
            }
            
//            context.write(new Text("sum"), new Text(sum.toString()));

        }
    }

    public static class JoinReduce
            extends Reducer<IntWritable, Text, Text, Text> {

        private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(JoinReduce.class);
        Configuration cache;
        int tablenum;
        BigInteger sum = new BigInteger("0");
        Integer table12sum = 0;
        Integer table23sum = 0;
        Integer table23filtersum = 0;
        Integer table12filtersum = 0;
        Integer copytimes = 0;
        String table2;
        String table3;
        ArrayList<ArrayList> tables = new ArrayList<ArrayList>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            LOG.info("reduce setup");
            cache = context.getConfiguration();
            table2 = cache.get("table2");
            table3 = cache.get("table3");
            cachetable(table2);
            cachetable(table3);
            tablenum = cache.getInt("tablesum", 0);

        }

        private void cachetable(String table) throws IOException {
            Path path = new Path(table);
            FSDataInputStream input = FileSystem.get(cache).open(path, 1024);
            BufferedReader br = new BufferedReader(new InputStreamReader(input));
            String line;
            ArrayList<String> row = new ArrayList<String>();
            while ((line = br.readLine()) != null) {
                row.add(line);
            }
            tables.add(row);
        }

        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            //context.write(key, new Text("join values start"));
            LOG.info("reduce");
            Iterator it = values.iterator();
            ArrayList<String> t2 = tables.get(0);
            ArrayList<String> t3 = tables.get(1);


            while (it.hasNext()) {
                context.progress();
                String f1 = it.next().toString();


                for (String f2 : t2) {
                    String[] fs2 = f2.split("\\|");
                    Integer l_orderkey = Integer.parseInt(f1.split("\\|")[0]);
                    Integer o_orderkey = Integer.parseInt(fs2[0]);
                    if (l_orderkey < o_orderkey) {
                        for (String f3 : t3) {
                            Integer o_custkey = Integer.parseInt(fs2[1]);
                            Integer c_custkey = Integer.parseInt(f3.split("\\|")[0]);
                            if (c_custkey > o_custkey) {
                                sum = sum.add(one);
                            }
                        }
                    }
                }
            }


            context.write(new Text("sum"), new Text(sum.toString()));

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: MRJ <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "MRJ");
        conf = job.getConfiguration();
        job.setJarByClass(Mapjoin.class);
        job.setMapperClass(JoinMap.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(JoinReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setPartitionerClass(MRJPartitioner.class);
        job.setNumReduceTasks(0);


        Path p = new Path(otherArgs[0]);
        FileInputFormat.addInputPath(job, p);
//        FSDataInputStream input = FileSystem.get(conf).open(p, 1024);
//        BufferedReader br = new BufferedReader(new InputStreamReader(input));
//        String line = "";
//        Integer lineCount = 0;
//        while ((line = br.readLine()) != null) {
//            lineCount++;
//        }
        //TODO cache
//        tablesizelist.add(lineCount);
//        conf.setInt("R" + 1, lineCount);

        conf.set("table2", otherArgs[1]);
//        conf.set("table3", otherArgs[2]);

//        int tablesum = otherArgs.length - 1;
//        conf.setInt("tablesum", tablesum);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(otherArgs[otherArgs.length - 1]));

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

//        int kr = 16;
//        r = kr;
//        job.setNumReduceTasks(kr);
//        conf.setInt("kr", kr);
//        job.setGroupingComparatorClass(mycomparator.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class mycomparator implements RawComparator {

        @Override
        public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
            return 0;
        }

        @Override
        public int compare(Object o1, Object o2) {
            return 0;
        }
    }
}

