package com.shopzilla.hadoop.demo.minimrcluster.multipleoutputs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

import static java.lang.String.format;

/**
 * @author Jeremy Lucas
 * @since 5/30/13
 */
public class MultipleOutputsReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    private static final String OUTPUT_PARTITION_PATTERN = "categoryId=%s/";

    private MultipleOutputs<LongWritable, Text> multipleOutputs;

    @Override
    protected void setup(final Context context) {
        multipleOutputs = new MultipleOutputs<LongWritable, Text>(context);
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    protected void reduce(final LongWritable categoryId, final Iterable<Text> products, final Context context) throws IOException, InterruptedException {
        int i = 0;
        final String outputPartition = generateOutputPartition(categoryId.get());
        for (final Text product : products) {
            multipleOutputs.write(new LongWritable(i++), product, outputPartition);
        }
    }

    private String generateOutputPartition(final long categoryId) {
        return format(OUTPUT_PARTITION_PATTERN, categoryId);
    }
}
