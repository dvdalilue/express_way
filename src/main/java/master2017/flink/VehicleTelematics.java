package master2017.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Comparator;

/**
 * Implements the "VehicleTelematics" program that analize the vehicles speed on
 * a certain express way with a record of events
 */
public class VehicleTelematics {
    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Event> events =
            env.readCsvFile(args[0]).pojoType(
                Event.class,
                "time",
                "vid",
                "speed",
                "xWay",
                "lane",
                "dir",
                "seg",
                "pos"
            );

        /**
         * Speed Radar
         */
        
        events

        .flatMap(new SpeedRadar())

        .groupBy(1)

        .first(1)

        .writeAsCsv(args[1] + "/speedfines.csv", "\n", ",", FileSystem.WriteMode.OVERWRITE);
        
        /**
         * Average Speed Control Segment
         */
        
        events

        .filter(new InsideControlSegment())

        .groupBy(new SelectorVID())

        .reduceGroup(new AvgInCompleteSegment())

        .filter(new AboveAvgSpeedSegment())

        .writeAsCsv(args[1] + "/avgspeedfines.csv", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        /**
         * Accidents
         */

        events

        .filter(new StopedVehicle())

        .groupBy(new SelectorVID())

        .sortGroup(new SelectorTime(), Order.ASCENDING)

        .reduceGroup(new SamePositionEvent())

        .flatMap(new AccidentVehicle())

        .writeAsCsv(args[1] + "/accidents.csv", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute("VehicleTelematics");
        } catch(Exception e)  {
            e.printStackTrace();
        }
    }

    //
    //  User Functions
    //

    public static final class AccidentVehicle
    implements FlatMapFunction<Object[],Accident> {
        public static void add(Event begin, Event end, Collector<Accident> o) {
            o.collect(
                new Accident(begin.time,end.time,begin.vid,begin.xWay,begin.seg,begin.dir,begin.pos)
            );
        }

        @Override
        public void flatMap(Object[] es, Collector<Accident> out) {
            for (int i = 3; i < es.length; i++) {
                if (
                    ((Event) es[i-3]).pos == 
                    ((Event) es[i-2]).pos &&
                    ((Event) es[i-2]).pos ==
                    ((Event) es[i-1]).pos &&
                    ((Event) es[i-1]).pos ==
                    ((Event) es[i]).pos 
                ) {
                    add((Event)es[i-3],(Event)es[i],out);
                }
            }
        }
    }

    /**
     * Implements a filter like function as flatMap. Collecting every event with
     * the speed field over 90mph and constructing a new instance of SpeedFine
     * with the desired fields on the data sink (CSV).
     */
    public static final class SpeedRadar
    implements FlatMapFunction<Event,SpeedFine> {
        @Override
        public void flatMap(Event e, Collector<SpeedFine> out) {
            if (e.speed > 90) {
                out.collect(new SpeedFine(e.time, e.vid, e.xWay, e.seg, e.dir, e.speed));
            }
        }
    }

    /**
     * Implements a filter function to discard events outside the control
     * segment of the express way. Leaving the dataSet only with events with the
     * field 'seg' between [52,56].
     */
    public static final class InsideControlSegment
    implements FilterFunction<Event> {
        @Override
        public boolean filter(Event e) {
            if (52 <= e.seg && e.seg <= 56) {
                return true;
            }
            return false;
        }
    }

    /**
     * Implements the filter function to keep the events with the speed above
     * the limit (60). It's used to check the avergae speed within a specific
     * segment.
     */
    public static final class AboveAvgSpeedSegment
    implements FilterFunction<AvgSpeedFine> {
        @Override
        public boolean filter(AvgSpeedFine e) {
            if (e.f5 > 60) {
                return true;
            }
            return false;
        }
    }

    public static final class StopedVehicle
    implements FilterFunction<Event> {
        @Override
        public boolean filter(Event e) {
            if (e.speed == 0) {
                return true;
            }
            return false;
        }
    }

    /**
     * Implements a key selector for the Event class. The unique identifier for
     * each event is the vehicle id (vid). 
     */
    public static class SelectorVID
    implements KeySelector<Event, Integer> {
        @Override
        public Integer getKey(Event e) {
            return e.vid;
        }
    }

    public static class SelectorTime
    implements KeySelector<Event, Integer> {
        @Override
        public Integer getKey(Event e) {
            return e.time;
        }
    }

    public static class SelectorVIDTime
    implements KeySelector<Event, VIDTime> {
        @Override
        public VIDTime getKey(Event e) {
            return (new VIDTime(e.vid,e.time));
        }
    }

    public static final class AvgInCompleteSegment
    implements GroupReduceFunction<Event,AvgSpeedFine> {
        @Override
        public void reduce(Iterable<Event> group, Collector<AvgSpeedFine> out) {
            int[] aux = {52,53,54,55,56};

            ArrayList<Integer> segments = new ArrayList<Integer>(6);

            for (int i : aux) { segments.add(i); }

            Iterator<Event> it = group.iterator();

            Event current = it.next();

            int time_min = current.time;
            int time_max = current.time;
            int vid = current.vid;
            int xWay = current.xWay;
            int dir = current.dir;

            int average = current.speed;
            int events = 1;

            while (it.hasNext()) {
                current = it.next();
                events += 1;

                segments.remove(new Integer(current.seg));

                if (current.time < time_min) { time_min = current.time; }
                if (current.time > time_max) { time_max = current.time; }
                average += current.speed;
            }

            average = average / events;

            if (segments.isEmpty()) {
                out.collect(new AvgSpeedFine(time_min,time_max,vid,xWay,dir,average));
            }
        }
    }

    public static final class SamePositionEvent
    implements GroupReduceFunction<Event,Object[]> {
        @Override
        public void reduce(Iterable<Event> group, Collector<Object[]> out) {
            Iterator<Event> it = group.iterator();

            Event previous = it.next();
            Event current;

            ArrayList<Event> event_list = new ArrayList<Event>();

            while (it.hasNext()) {
                current = it.next();
                event_list.add(previous);

                if (previous.pos == current.pos && !it.hasNext()) {
                    event_list.add(current);
                }
                previous = current;
            }

            out.collect(event_list.toArray());
        }
    }
}