package master2017.flink;

import org.apache.flink.api.java.tuple.Tuple6;

public class SpeedFine extends Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> {
    public SpeedFine() {
        super();
    }

    public SpeedFine(int a, int b, int c, int d, int e, int f) {
        super(a,b,c,d,e,f);
    }
}