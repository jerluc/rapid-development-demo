package com.shopzilla.hadoop.demo.minimrcluster.multipleoutputs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.OutputStream;

import static com.google.common.base.Throwables.propagate;
import static org.junit.Assert.assertTrue;

/**
 * @author Jeremy Lucas
 * @since 5/30/13
 */
public class MultipleOutputsRunnerTest {
    static {
        System.setProperty("hadoop.log.dir", "/tmp/minimr-logs");
    }

    MiniMRCluster mrCluster;

    MiniDFSCluster dfsCluster;

    FileSystem fs;

    Configuration configuration;

    MultipleOutputsRunner runner;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        dfsCluster = new MiniDFSCluster(configuration, 2, true, null);
        mrCluster = new MiniMRCluster(2, dfsCluster.getFileSystem().getUri().toString(), 1);
        runner = new MultipleOutputsRunner(configuration);
        fs = FileSystem.get(configuration);
    }

    @After
    public void tearDown() throws Exception {
        final Thread shutdownThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    if (mrCluster != null) {
                        mrCluster.shutdown();
                        mrCluster = null;
                    }
                    if (dfsCluster != null) {
                        dfsCluster.shutdown();
                        dfsCluster = null;
                    }
                } catch (final Exception ex) {
                    throw propagate(ex);
                }
            }
        });
        shutdownThread.start();
        shutdownThread.join(10000);
    }

    void setupData(final String...inputs) throws Exception {
        fs.mkdirs(new Path("/data/input"));
        fs.mkdirs(new Path("/data/output"));
        for (final String input : inputs) {
            final OutputStream outputStream = fs.create(new Path("/data/input" + input));
            IOUtils.copy(MultipleOutputsRunnerTest.class.getResourceAsStream(input), outputStream);
            IOUtils.closeQuietly(outputStream);
        }
    }

    @Test
    public void testLaunchJob() throws Exception {
        setupData("/input1.txt", "/input2.txt");
        assertTrue(runner.launchJob("/data/input", "/data/output", 1));
        for (final FileStatus categoryPartition : fs.globStatus(new Path("/data/output/categoryId*"))) {
            System.out.println(categoryPartition.getPath());
            for (final FileStatus fileStatus : fs.listStatus(categoryPartition.getPath())) {
                System.out.println(fileStatus.getPath());
                System.out.println(IOUtils.toString(fs.open(fileStatus.getPath())));
            }
            Thread.sleep(10000);
        }
        Thread.sleep(5000);
    }
}
