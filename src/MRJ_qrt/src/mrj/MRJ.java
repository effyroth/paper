/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mrj;

/**
 *
 * @author Thinkpad
 */
import java.io.*;
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
import java.math.BigInteger;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.TableLine;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.util.ReflectionUtils;

public class MRJ {
    
    static final BigInteger one = new BigInteger("1");
    static Integer r;
    static Integer F1;
    static Integer D1;
    static Integer F1segments;
    static Integer D1segments;
    static Integer bucketsize;
    static Integer F1sidelength;
    static Integer D1sidelength;
    
    public static class JoinMap
            extends Mapper<Object, Text, TableLine, Text> {

        //private final static IntWritable one = new IntWritable(1);
        private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(JoinMap.class);
        private TableLine tableline;
        private Text value = new Text();
        private Configuration cache;
        private BigInteger ht = new BigInteger("100000");
        boolean first = true;
        Integer F1sidelength;
        Integer D1sidelength;
        Integer F1segments;
        Integer D1segments;
    
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            cache = context.getConfiguration();
            F1segments = cache.getInt("F1segments", 1);
            D1segments = cache.getInt("D1segments", 1);
            F1sidelength = Integer.parseInt(cache.get("F1sidelength"));
            D1sidelength = Integer.parseInt(cache.get("D1sidelength"));
            LOG.info("map setup");

        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String s = null;
            int table = 0;
            BigInteger line = null;
            try{
                table = Integer.parseInt(itr.nextToken());
                line = new BigInteger(itr.nextToken());

                tableline = new TableLine(table,line);
                s = itr.nextToken();
            }catch(NoSuchElementException e){
                System.out.println("NoSuchElementException:"+"table:"+ table +" line:" + line + " s:" + s);
            }
            if(s == null){
                return;
            }
            
            BigInteger bucketsize = new BigInteger(cache.get("bucketsize", "0"));
            int tablesum = cache.getInt("tablesum", 0);

//            tableline.bucketset = bucketset(tableline.table, tableline.line, tablesum, bucketsize);
            tableline.bucketset = bucket(tableline.table, tableline.line.intValue());
            
            while (itr.hasMoreTokens()) {
                s += " " + itr.nextToken();
            }
            this.value.set(s);
            //System.out.println(s);
            context.write(tableline, this.value);
            //System.out.println("map write ok" + ((JobConf) context.getConfiguration()).getNumMapTasks()
            //        + ((JobConf) context.getConfiguration()).getNumTasksToExecutePerJvm());
        }

        public Set bucket(int t, int l){
            int position = 0;
            Set<Integer> s = new HashSet<Integer>();
            if(t == 1){
                position = (l - 1) / F1sidelength;
                for(int i = 0; i < D1segments; i++){
                    s.add(position + i * F1segments);
                }
            }else{
                position = (l - 1) / D1sidelength;
                int start = (position) * F1segments;
                for(int i = 0; i < F1segments; i++){
                    s.add(start + i);
                }
            }
            return s;
        }
        
    }

    public static class JoinReduce
            extends Reducer<TableLine, Text, Text, Text> {

        private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(JoinReduce.class);
        
        private Text result = new Text();
        JobConf cache;
        List<ReduceContext> list = new ArrayList<ReduceContext>();
        //BigInteger bi = new BigInteger("");
        int tablenum;
        //Cloner c = new Cloner();
        Path[] p;
        //MyContext<TableLine, Text, TableLine, Text> mycontext;
        FileSystem rfs;
        static BigInteger sum = new BigInteger("0");
        //Kryo c = new Kryo();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            LOG.info("reduce setup");
            cache = (JobConf) context.getConfiguration();
            cache.setBoolean("nio", true);
//            rfs = FileSystem.getLocal(cache).getRaw();
            tablenum = cache.getInt("tablesum", 0);

        }

        private CompressionCodec initCodec() {
            // check if map-outputs are to be compressed
            if (cache.getCompressMapOutput()) {
                Class<? extends CompressionCodec> codecClass =
                        (cache).getMapOutputCompressorClass(DefaultCodec.class);
                return ReflectionUtils.newInstance(codecClass, cache);
            }

            return null;
        }


        public void reduce(TableLine key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            //context.write(key, new Text("join values start"));
            LOG.info("reduce");
            int i = 1;
            //Reducer.Context.class.toString();
            do {
                //context.write(key, new Text("join values" + i));
                //context.write(mycontext.getCurrentKey(), mycontext.getCurrentValue());
                ReduceContext rc = context.copy(cache);
                if(rc == null){
                    //LOG.info("rc:null");
                }else{
                    //LOG.info(rc);
                }
                list.add(rc);
                
                //LOG.info(i++);
            } while (context.nextKey());
            LOG.info("clone over list:" + list.size());
            String s = "";
            ArrayList<String[]> tables = new ArrayList<String[]>();
            joinAndCollect(0, tables, context);

            context.write(new Text("sum"), new Text(sum.toString()));
        }

        private void joinAndCollect(int pos, ArrayList<String[]> tables,
                Context context) throws IOException, InterruptedException {
            //LOG.info("joinAndCollect:pos=" + pos);
            context.progress();
            if (tablenum == pos) {
                if(FilterExecute(pos,tables)){
                    return;
                }else{
                    sum = sum.add(one);
                }
                
//                String out = "";
//                for(String[] fields : tables){
//                    for(String field : fields){
//                        out += field + " ";
//                    }
//                }            
//                context.write(new Text(""), new Text(out));
                //System.out.println("write " + s);
                //s.remove(pos - 1);
                return;
            }
            //LOG.info("copy");
            ReduceContext con = list.get(pos).copy(cache);
            //ReduceContext con = c.deepClone(list.get(pos));
            Iterator it = con.getValues().iterator();
            boolean hasnext = it.hasNext();
            //System.out.println("hasnext " + hasnext);
            while (hasnext) {
                String v = it.next().toString();
                String fields[] = v.split("\\|");
                tables.add(fields);
                //if(pos == 0)
                    //LOG.info("i.n " + v);
                //s += v;
                joinAndCollect(pos + 1, tables, context);
                tables.remove(pos);
                hasnext = it.hasNext();
                //System.out.println("hasnext " + hasnext);
            }
        }
        
        private boolean FilterExecute(int pos, ArrayList<String[]> tables){
            Integer l_orderkey = Integer.parseInt(tables.get(1)[0]);
            Integer o_orderkey = Integer.parseInt(tables.get(0)[0]);
            if (l_orderkey < o_orderkey) {
                return false;
            }
            return true;
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
        job.setJarByClass(MRJ.class);
        job.setMapperClass(JoinMap.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(JoinReduce.class);
        job.setMapOutputKeyClass(TableLine.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setPartitionerClass(MRJPartitioner.class);


        for (int i = 1; i < otherArgs.length; ++i) {
            Path p = new Path(otherArgs[i - 1]);
            FileInputFormat.addInputPath(job, p);
            FSDataInputStream input = FileSystem.get(conf).open(p, 1024);
            BufferedReader br = new BufferedReader(new InputStreamReader(input));
            String line = "";
            BigInteger lineCount = new BigInteger("0");
            while ((line = br.readLine()) != null) {
                lineCount = lineCount.add(one);
            }
            //TODO cache
            conf.set("R" + i, lineCount.toString());
        }



        conf.setInt("tablesum", otherArgs.length - 1);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(otherArgs[otherArgs.length - 1]));
        
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        Integer kr = 16;
        r = kr;
//        job.setNumReduceTasks(kr);
//        conf.setInt("kr", kr);
        conf.setBoolean("join", true);

        F1 = Integer.parseInt(conf.get("R1", "1"));
        D1 = Integer.parseInt(conf.get("R2", "1"));
        Long sum = F1.longValue() * D1.longValue();
        Double bucketside = Math.sqrt( sum / kr);
        F1segments = (F1 - 1)  / bucketside.intValue() + 1;
        D1segments = (D1 - 1)  / bucketside.intValue() + 1;
        kr = F1segments * D1segments;
        job.setNumReduceTasks(kr);
        conf.setInt("kr", kr);
        F1sidelength = (F1 - 1) / F1segments + 1;
        D1sidelength = (D1 - 1) / D1segments + 1;
        
//        double proportion = F1/D1;
//        double F1partition = Math.sqrt(proportion * r);
//        F1segments = BestPartitionAmount(F1partition);
//        D1segments = r / F1segments;
//        F1sidelength = (F1 - 1) / F1segments + 1;
//        D1sidelength = (D1 - 1) / D1segments + 1;
        
        conf.setInt("F1segments", F1segments);
        conf.setInt("D1segments", D1segments);
        conf.set("F1sidelength", F1sidelength.toString());
        conf.set("D1sidelength", D1sidelength.toString());
        //job.setGroupingComparatorClass(mycomparator.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static Integer BestPartitionAmount(double x){
        int approximation = Approximation(x);
//        Integer bestx = Compare(approximation,approximation);
        return approximation;
    }
    
    public static Integer Compare(Integer a, Integer b){
//        Integer i = null;   
        Integer left = a * D1 + r / a * F1;
        Integer right = b * D1 + r / b * F1;
        if(left > right){
            return b;
        }
        return a;
//        return i;
    }
    
    public static int Approximation(double x){
        int approximation = 1;
        
        List<Integer> l = getFactorList(r);
        
        if(x <= 1){
            approximation = 1;
        }else if(x >= r){
            approximation = r;
        }else{
            for(int i = 1; i < l.size(); i++){
                if( l.get(i) > x){
                    approximation = Compare(l.get(i - 1), l.get(i));
                    break;
                }
            }
        }
        return approximation;
    }
    
    public static List<Integer> getFactorList(int r){
        List<Integer> l = new ArrayList<Integer>();
        int sqrt = (int) Math.sqrt(r);
        for(int i = sqrt; i > 0; i--){
            if(r % i == 0){
                l.add(0, i);
                if(r/i != i){
                    l.add(r/i);
                }
            }
        }
        return l;
    }

}
