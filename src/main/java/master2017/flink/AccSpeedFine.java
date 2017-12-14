package master2017.flink;

import java.util.ArrayList;
import org.apache.flink.api.java.functions.KeySelector;

public class AccSpeedFine {
    public long min;
    public long max;
    public int vid;
    public int speed;
    public int xWay;
    public int dir;
    public int n_event;
    public ArrayList<Integer> segments;

    public AccSpeedFine(Event e) {
        this.min = e.time;
        this.max = e.time;
        this.vid = e.vid;
        this.speed = e.speed;
        this.xWay = e.xWay;
        this.dir = e.dir;
        this.n_event = 1;
        this.segments = new ArrayList<Integer>();
        this.segments.add(e.seg);
    }

    /**
     * Implements a key selector for the AccSpeedFine class. The unique
     * identifier for each AccSpeedFine event is the vehicle id (vid). 
     */
    public static class SelectorVID
    implements KeySelector<AccSpeedFine, Integer> {
        @Override
        public Integer getKey(AccSpeedFine e) {
            return e.vid;
        }
    }
}