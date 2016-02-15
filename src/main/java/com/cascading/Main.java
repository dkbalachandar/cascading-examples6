
package com.cascading;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A Cascading example to read a tsv file file which contains user name, age and dept details and remove the users whose age is more than or equals to 30
 * and do group by deptId and output the deptId and count in a tsv file
 */
public class Main {


    /**
     * This examples uses SubAssembly. ExpressFilter and TapsMap
     *
     * @param args
     */
    public static void main(String[] args) {

        System.out.println("Hdfs Job Starts");
        //input and output path
        String inputPath = args[0];
        String outputPath = args[1];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);

        //Create the source tap
        Fields fields = new Fields("userName", "age", "deptId");
        Tap inTap = new Hfs(new TextDelimited(fields, true, "\t"), inputPath);

        //Create the sink tap
        Tap outTap = new Hfs(new TextDelimited(true, "\t"), outputPath, SinkMode.REPLACE);

        //Create input taps map
        Map tapsMap = new HashMap();
        tapsMap.put(UserSubAssembly.INPUT_PIPE_NAME, inTap);

        //SubAssembly
        Pipe pipe = new UserSubAssembly();

        //Create the flow
        Flow flow = new HadoopFlowConnector().connect(tapsMap, outTap, pipe);
        flow.complete();
        System.out.println("Hdfs Job Ends");
    }
}

