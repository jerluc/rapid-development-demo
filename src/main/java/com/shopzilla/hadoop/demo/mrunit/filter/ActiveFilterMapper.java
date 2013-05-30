/**
 * Copyright (C) 2004 - 2013 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */

package com.shopzilla.hadoop.demo.mrunit.filter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Jeremy Lucas
 * @since 5/30/13
 */
public class ActiveFilterMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
        if (value.toString().contains("ACTIVE")) {
            context.write(key, value);
        }
    }
}
