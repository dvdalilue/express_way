package master2017.flink;

import org.apache.flink.api.java.tuple.Tuple2;

public class VIDTime extends Tuple2<Integer,Integer> {
    public VIDTime() {
        super();
    }

    public VIDTime(int a, int b) {
        super(a,b);
    }
}