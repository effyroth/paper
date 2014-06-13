/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce;

import com.esotericsoftware.minlog.Log;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.TableLine;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.util.Progressable;


/**
 * The context passed to the {@link Reducer}.
 * @param <KEYIN> the class of the input keys
 * @param <VALUEIN> the class of the input values
 * @param <KEYOUT> the class of the output keys
 * @param <VALUEOUT> the class of the output values
 */
public class ReduceContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    extends TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(ReduceContext.class);
    
  private RawKeyValueIterator input;
  private Counter inputKeyCounter;
  private Counter inputValueCounter;
  private RawComparator<KEYIN> comparator;
  private KEYIN key;                                  // current key
  private VALUEIN value;                              // current value
  private boolean firstValue = false;                 // first value in key
  private boolean nextKeyIsSame = false;              // more w/ this key
  private boolean hasMore;                            // more in file
  protected Progressable reporter;
  private Deserializer<KEYIN> keyDeserializer;
  private Deserializer<VALUEIN> valueDeserializer;
  private DataInputBuffer buffer = new DataInputBuffer();
  private BytesWritable currentRawKey = new BytesWritable();
  private ValueIterable iterable = new ValueIterable();

  public ReduceContext(Configuration conf, TaskAttemptID taskid,
                       RawKeyValueIterator input, 
                       Counter inputKeyCounter,
                       Counter inputValueCounter,
                       RecordWriter<KEYOUT,VALUEOUT> output,
                       OutputCommitter committer,
                       StatusReporter reporter,
                       RawComparator<KEYIN> comparator,
                       Class<KEYIN> keyClass,
                       Class<VALUEIN> valueClass
                       ) throws InterruptedException, IOException{
    super(conf, taskid, output, committer, reporter);
    this.input = input;
    this.inputKeyCounter = inputKeyCounter;
    this.inputValueCounter = inputValueCounter;
    this.comparator = comparator;
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
    this.keyDeserializer.open(buffer);
    this.valueDeserializer = serializationFactory.getDeserializer(valueClass);
    this.valueDeserializer.open(buffer);
    hasMore = input.next();
  }
  
  public ReduceContext(Configuration conf, TaskAttemptID taskid,
                        RawKeyValueIterator input, 
                        RawComparator<KEYIN> comparator,
                        DataInputBuffer buffer,
                        Class<KEYIN> keyClass,
                        Class<VALUEIN> valueClass) throws IOException{
      super(conf,taskid,null,null,null);
      this.input = input;
      this.comparator = comparator;
      this.buffer = buffer;
      SerializationFactory serializationFactory = new SerializationFactory(conf);
        this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
        this.keyDeserializer.open(this.buffer);
        this.valueDeserializer = serializationFactory.getDeserializer(valueClass);
        this.valueDeserializer.open(this.buffer);
        //LOG.info("ReduceContext construct over");
        
  }
  
  public ReduceContext copy(Configuration conf) throws IOException, InterruptedException{
//      LOG.info("ReduceContext:copy");
      ReduceContext copy = new ReduceContext(conf,
                                new TaskAttemptID(),
                                (this.input).copy(conf),
                                this.comparator,
                                this.buffer.copy(),
                                TableLine.class,
                                Text.class);
      copy.firstValue = true;
      copy.nextKeyIsSame = this.nextKeyIsSame;
      copy.hasMore = this.hasMore;
      copy.key = this.key;
      if(this.value == null){
          copy.value = null;
      }else{
        copy.value = new Text(this.value.toString());
      }
      copy.currentRawKey = WritableUtils.clone(this.currentRawKey, conf);
      
      return copy;
  }

  /** Start processing next unique key. */
  public boolean nextKey() throws IOException,InterruptedException {
    while (hasMore && nextKeyIsSame) {
      nextKeyValue();
    }
    if (hasMore) {
      if (inputKeyCounter != null) {
        inputKeyCounter.increment(1);
      }
      return nextKeyValue();
    } else {
      return false;
    }
  }

  /**
   * Advance to the next key/value pair.
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!hasMore) {
      key = null;
      value = null;
      return false;
    }
    firstValue = !nextKeyIsSame;
    DataInputBuffer next = input.getKey();

    
    currentRawKey.set(next.getData(), next.getPosition(), 
                      next.getLength() - next.getPosition());
    
//    byte by[] = currentRawKey.getBytes();
//    for(byte b : by){
//        LOG.info("currentRawKey " + Byte.toString(b));
//    }
    
    buffer.reset(currentRawKey.getBytes(), 0, currentRawKey.getLength());
    
//    byte by[] = buffer.getData();
//    for(byte b : by){
//        LOG.info("buffer " + Byte.toString(b));
//    }    
    
    key = keyDeserializer.deserialize(key);
    next = input.getValue();
    buffer.reset(next.getData(), next.getPosition(), next.getLength());
    value = valueDeserializer.deserialize(value);
    hasMore = input.next();
    
//    LOG.info("hasMore " + hasMore);
    
    if (hasMore) {
      next = input.getKey();
      nextKeyIsSame = comparator.compare(currentRawKey.getBytes(), 0, 
                                         currentRawKey.getLength(),
                                         next.getData(),
                                         next.getPosition(),
                                         next.getLength() - next.getPosition()
                                         ) == 0;
    } else {
      nextKeyIsSame = false;
    }
    if(inputValueCounter != null)
        inputValueCounter.increment(1);
    return true;
  }

  public KEYIN getCurrentKey() {
    return key;
  }

  @Override
  public VALUEIN getCurrentValue() {
    return value;
  }

  protected class ValueIterator implements Iterator<VALUEIN> {

    @Override
    public boolean hasNext() {
      return firstValue || nextKeyIsSame;
    }

    @Override
    public VALUEIN next() {
      // if this is the first record, we don't need to advance
      if (firstValue) {
        firstValue = false;
        return value;
      }
      // if this isn't the first record and the next key is different, they
      // can't advance it here.
      if (!nextKeyIsSame) {
        //throw new NoSuchElementException("iterate past last value");
      }
      // otherwise, go to the next key/value pair
      try {
        nextKeyValue();
        return value;
      } catch (IOException ie) {
          ie.printStackTrace();
        throw new RuntimeException("next value iterator failed" + ie);
         
      } catch (InterruptedException ie) {
        // this is bad, but we can't modify the exception list of java.util
        throw new RuntimeException("next value iterator interrupted", ie);        
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove not implemented");
    }
    
  }

  protected class ValueIterable implements Iterable<VALUEIN> {
    private ValueIterator iterator = new ValueIterator();
    @Override
    public Iterator<VALUEIN> iterator() {
      return iterator;
    } 
  }
  
  /**
   * Iterate through the values for the current key, reusing the same value 
   * object, which is stored in the context.
   * @return the series of values associated with the current key. All of the 
   * objects returned directly and indirectly from this method are reused.
   */
  public 
  Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
    return iterable;
  }
}
