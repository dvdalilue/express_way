package master2017.flink;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.common.functions.MapFunction;

public final class Event {
    public int time;
    public int vid;
    public int speed;
    public int xWay;
    public int lane;
    public int dir;
    public int seg;
    public int pos;

    public Event() { }

    public Event(int f0, int f1, int f2, int f3, int f4, int f5, int f6, int f7) {
        this.time  = f0;
        this.vid   = f1;
        this.speed = f2;
        this.xWay  = f3;
        this.lane  = f4;
        this.dir   = f5;
        this.seg   = f6;
        this.pos   = f7;
    }

    public Event(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> t) {
        this.time = t.f0;
        this.vid = t.f1;
        this.speed = t.f2;
        this.xWay = t.f3;
        this.lane = t.f4;
        this.dir = t.f5;
        this.seg = t.f6;
        this.pos = t.f7;
    }

    public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> toTuple() {
        return (new Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>(
            time,
            vid,
            speed,
            xWay,
            lane,
            dir,
            seg,
            pos
        ));
    }

    public static final class toEvent implements MapFunction<String, Event> {
        @Override
        public Event map(String in) {
            String[] s = in.split(",");

            return (new Event(
                Integer.parseInt(s[0]),
                Integer.parseInt(s[1]),
                Integer.parseInt(s[2]),
                Integer.parseInt(s[3]),
                Integer.parseInt(s[4]),
                Integer.parseInt(s[5]),
                Integer.parseInt(s[6]),
                Integer.parseInt(s[7]))
            );
        }
    }

    // public static final class toTuple implements MapFunction<Event, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> {
    //     @Override
    //     public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> map(Event in) {
    //         return (in.toTuple());
    //     }
    // }
}