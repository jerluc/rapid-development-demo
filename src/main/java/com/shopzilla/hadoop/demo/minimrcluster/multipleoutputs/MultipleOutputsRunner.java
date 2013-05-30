package com.shopzilla.hadoop.demo.minimrcluster.multipleoutputs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import static com.google.common.base.Throwables.propagate;

/**
 * @author Jeremy Lucas
 * @since 5/30/13
 */
public class MultipleOutputsRunner {

    protected final Configuration configuration;

    public MultipleOutputsRunner(final Configuration configuration) {
        this.configuration = configuration;
    }

    public boolean launchJob(final String inputPath, final String outputPath, final int numReduceTasks) {
        try {
            final FileSystem fs = FileSystem.get(configuration);
            if (fs.exists(new Path(outputPath))) {
                fs.delete(new Path(outputPath), true);
            }

            final Job job = new Job(configuration);
            job.setJarByClass(MultipleOutputsRunner.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setMapperClass(MultipleOutputsMapper.class);
            job.setReducerClass(MultipleOutputsReducer.class);
            job.setNumReduceTasks(numReduceTasks);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            return job.waitForCompletion(true);
        } catch (final Exception ex) {
            throw propagate(ex);
        }
    }
}
