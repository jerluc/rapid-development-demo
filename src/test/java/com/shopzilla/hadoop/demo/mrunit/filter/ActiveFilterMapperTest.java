package com.shopzilla.hadoop.demo.mrunit.filter;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Jeremy Lucas
 * @since 5/30/13
 */
public class ActiveFilterMapperTest {
    @Test
    public void testMapPassFilter() throws Exception {
        final MapDriver<Text, Text, Text, Text> driver = new MapDriver<Text, Text, Text, Text>();
        driver.setMapper(new ActiveFilterMapper());
        final Pair<Text, Text> inputPair = new Pair<Text, Text>(new Text("key"), new Text("value\tACTIVE"));
        driver.setInput(inputPair);
        assertEquals(Lists.newArrayList(inputPair), driver.run());
    }

    @Test
    public void testMapReject() throws Exception {
        final MapDriver<Text, Text, Text, Text> driver = new MapDriver<Text, Text, Text, Text>();
        driver.setMapper(new ActiveFilterMapper());
        final Pair<Text, Text> inputPair = new Pair<Text, Text>(new Text("key"), new Text("value\tPAUSED"));
        driver.setInput(inputPair);
        assertEquals(Lists.<Pair<Text, Text>>newArrayList(), driver.run());
    }
}
