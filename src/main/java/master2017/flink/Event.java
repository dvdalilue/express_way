package master2017.flink;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.functions.KeySelector;

public final class Event {
    public long time;
    public int vid;
    public int speed;
    public int xWay;
    public int lane;
    public int dir;
    public int seg;
    public int pos;

    public Event(long f0, int f1, int f2, int f3, int f4, int f5, int f6, int f7) {
        this.time  = f0;
        this.vid   = f1;
        this.speed = f2;
        this.xWay  = f3;
        this.lane  = f4;
        this.dir   = f5;
        this.seg   = f6;
        this.pos   = f7;
    }

    public Event(Tuple8<Long,Integer,Integer,Integer,Integer,Integer,Integer,Integer> t) {
        this.time  = t.f0;
        this.vid   = t.f1;
        this.speed = t.f2;
        this.xWay  = t.f3;
        this.lane  = t.f4;
        this.dir   = t.f5;
        this.seg   = t.f6;
        this.pos   = t.f7;
    }

    /**
     * Implements a key selector for the Event class. The unique identifier for
     * each event is the vehicle id (vid). 
     */
    public static final class SelectorVID
    implements KeySelector<Event, Integer> {
        @Override
        public Integer getKey(Event e) {
            return e.vid;
        }
    }
}