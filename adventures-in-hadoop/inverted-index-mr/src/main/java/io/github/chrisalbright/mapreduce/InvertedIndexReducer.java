package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
  @Override
  protected void reduce(Text word, Iterable<Text> countsPerDocument, Context context) throws IOException, InterruptedException {
      throw new RuntimeException("Not Implemented");
  }
}