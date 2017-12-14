package master2017.flink;

import org.apache.flink.api.java.tuple.Tuple2;

public class VIDTime extends Tuple2<Integer,Long> {
    public VIDTime() {
        super();
    }

    public VIDTime(int a, long b) {
        super(a,b);
    }
}