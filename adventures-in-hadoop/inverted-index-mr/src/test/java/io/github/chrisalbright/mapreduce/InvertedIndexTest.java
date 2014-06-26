package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

public class InvertedIndexTest {

  @Test
  public void testMap() throws IOException {
    MapDriver<LongWritable, Text, Text, Text> mapDriver =
        new MapDriver<LongWritable, Text, Text, Text>(new InvertedIndexMapper())
            .withInput(new LongWritable(1), new Text("The quick brown fox jumps over the lazy dog"))
            .withOutput(new Text("the"), new Text("somefile,1"))
            .withOutput(new Text("quick"), new Text("somefile,1"))
            .withOutput(new Text("brown"), new Text("somefile,1"))
            .withOutput(new Text("fox"), new Text("somefile,1"))
            .withOutput(new Text("jumps"), new Text("somefile,1"))
            .withOutput(new Text("over"), new Text("somefile,1"))
            .withOutput(new Text("the"), new Text("somefile,1"))
            .withOutput(new Text("lazy"), new Text("somefile,1"))
            .withOutput(new Text("dog"), new Text("somefile,1"));

    boolean checkOutputOrder = false;
    mapDriver.runTest(checkOutputOrder);
  }

  @Test
  public void testReduce() throws IOException {
      throw new RuntimeException("Not Implemented");
  }

  @Test
  public void testMapReduce() throws IOException {
      throw new RuntimeException("Not Implemented");
  }
}
