package master2017.flink;

import org.apache.flink.api.java.tuple.Tuple7;

public class Accident extends Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer> {
    public Accident() {
        super();
    }

    public Accident(int a, int b, int c, int d, int e, int f, int g) {
        super(a,b,c,d,e,f,g);
    }
}