package com.cascading;

import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

public class UserSubAssembly extends SubAssembly {

    private static final long serialVersionUID = 3070079050769273334L;

    public static final String INPUT_PIPE_NAME = "input";

    public static final String OUTPUT_PIPE_NAME = "output";

    public UserSubAssembly() {
        this(new Pipe(INPUT_PIPE_NAME), OUTPUT_PIPE_NAME);
    }

    public UserSubAssembly(Pipe input, String tailName) {

        super();
        Pipe pipe = new Each(input, new Fields("age"),
                new ExpressionFilter("age >= 30", Integer.TYPE));
        pipe = new GroupBy(pipe, new Fields("deptId"));
        pipe = new Every(pipe, new Count());
        pipe = new Pipe(tailName, pipe);
        setTails(pipe);
    }
}
