package com.shopzilla.hadoop.demo.mrunit.join;

import com.google.common.collect.Sets;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Jeremy Lucas
 * @since 5/30/13
 */
public class JoinReducerTest {
    @Test
    public void testReduce() throws Exception {
        final List<Pair<Text, Text>> output = new MapReduceDriver<Text, Text, Text, Text, Text, Text>()
            .withMapper(new Mapper<Text, Text, Text, Text>())
            .withReducer(new JoinReducer())
            .withInput(new Text("a"), new Text("1"))
            .withInput(new Text("b"), new Text("2"))
            .withInput(new Text("a"), new Text("3"))
            .run();
        assertEquals(Sets.<Pair<Text, Text>>newHashSet(
            new Pair<Text, Text>(new Text("a"), new Text("1;3")),
            new Pair<Text, Text>(new Text("b"), new Text("2"))
        ), Sets.newHashSet(output));
    }
}
