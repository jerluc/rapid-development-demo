package com.shopzilla.hadoop.demo.mrunit.join;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Jeremy Lucas
 * @since 5/30/13
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    private static final Joiner JOINER = Joiner.on(";");

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        final Iterable<String> valuesAsText = Iterables.transform(values, new Function<Text, String>() {
            @Override
            public String apply(final Text input) {
                return input.toString();
            }
        });
        context.write(key, new Text(JOINER.join(valuesAsText)));
    }
}
