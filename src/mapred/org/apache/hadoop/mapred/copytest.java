/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.TableLine;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;

/**
 *
 * @author hadoop
 */
public class copytest {
    public static void main(String args[]) throws IOException, URISyntaxException, InterruptedException{
          //int b = 1;
          System.out.println("123");
          Configuration conf = new Configuration();
          conf.setBoolean("nio", true);
          JobConf job = new JobConf(conf);
          RawLocalFileSystem fs = new RawLocalFileSystem();
          fs.setConf(conf);
          URI uri = new URI("");
          fs.initialize(uri, conf);
          System.out.println(fs.getWorkingDirectory());
          Path[] p = new Path[2];
          p[0] = new Path("big1");
          p[1] = new Path("big2");
//          byte b[] = new byte[10];
//          FSDataInputStream in = fs.open(p[0]);
//                  in.read(b, 0, b.length);
          RawKeyValueIterator it = Merger.merge( conf,  fs,
                             TableLine.class,  Text.class, 
                             null,
                            p, false, 
                             100, new Path("tmp"),
                            (RawComparator<TableLine>)job.getOutputKeyComparator(),
                            null,
                            null,
                            null);
          ReduceContext rc = new ReduceContext(conf, new TaskAttemptID(),
                       it, 
                       null,
                       null,
                       null,
                       null,
                       null,
                       (RawComparator<TableLine>)job.getOutputKeyComparator(),
                       TableLine.class,  Text.class);
         ReduceContext copy1 = rc.copy(conf);
         rc.nextKey();
//         rc.nextKey();
//         rc.nextKey();
         ReduceContext copy2 = rc.copy(conf);
         Iterator i1 = copy1.getValues().iterator();
         Iterator i2 = copy2.getValues().iterator();
         int count = 0;
//         while(true){
//             count++;
//             i2.next();
//         }
//         System.out.println(count);
//         count = 0;
//         while(i1.hasNext()){
//             count++;
//             i1.next();
//         }
//         System.out.println(count);
         //i.next();
         //int z = 0;
      }
}
