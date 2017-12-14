package master2017.flink;

import org.apache.flink.api.java.functions.KeySelector;
import java.util.ArrayList;

public class AccSpeedFine {
    public long time1;
    public long time2;
    public int vid;
    public int speed;
    public int xWay;
    public int dir;
    public int n_event;
    public ArrayList<Integer> segments;

    public AccSpeedFine(Event e) {
        this.time1 = e.time;
        this.time2 = e.time;
        this.vid = e.vid;
        this.speed = e.speed;
        this.xWay = e.xWay;
        this.dir = e.dir;
        this.n_event = 1;
        this.segments = new ArrayList<Integer>();
        this.segments.add(e.seg);
    }

    public static class SelectorVID
    implements KeySelector<AccSpeedFine, Integer> {
        @Override
        public Integer getKey(AccSpeedFine e) {
            return e.vid;
        }
    }
}