package com.shopzilla.hadoop.demo.minimrcluster.multipleoutputs;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Jeremy Lucas
 * @since 5/30/13
 */
public class MultipleOutputsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private static final Splitter KEY_VALUE_SPLITTER = Splitter.on('\t').limit(2);

    @Override
    public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        final Iterable<String> recordParts = KEY_VALUE_SPLITTER.split(value.toString());
        final long categoryId = Long.parseLong(Iterables.get(recordParts, 0));
        final String product = Iterables.get(recordParts, 1);
        context.write(new LongWritable(categoryId), new Text(product));
    }
}
