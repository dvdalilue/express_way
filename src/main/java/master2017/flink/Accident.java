package master2017.flink;

import org.apache.flink.api.java.tuple.Tuple7;

public class Accident extends Tuple7<Long,Long,Integer,Integer,Integer,Integer,Integer> {
    public Accident() {
        super();
    }

    public Accident(long a, long b, int c, int d, int e, int f, int g) {
        super(a,b,c,d,e,f,g);
    }
}