package master2017.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> events =
            env.readTextFile(args[0])

            // .setParallelism(env.getParallelism())

            .map(new Event.toEvent());

            // .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
            //     @Override
            //     public long extractAscendingTimestamp(Event e) {
            //         return (e.time * 1000);
            //     }
            // });

        /**
         * Speed Radar
         */
        
        // events

        // .windowAll(TumblingEventTimeWindows((long)100,(long)0))

        // .flatMap(new SpeedRadar())

        // .keyBy(new SelectorVID())

        // .window(TumblingEventTimeWindows.of(Time.seconds(100)))

        // .apply(new WindowFunction<Event, SpeedFine, Integer, TimeWindow>() {
        //     public void apply(Integer key, TimeWindow window, Iterable<Event> events, Collector<SpeedFine> out) {
        //         System.out.println("-------> " + key);
        //         // for (Event e : events) {
        //         //     if (e.speed > 90) { out.collect(new SpeedFine(e.time, e.vid, e.xWay, e.seg, e.dir, e.speed)); }
        //         // }
        //     }
        // })

        // .writeAsCsv(args[1] + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE, "\n", ",");
        
        /**
         * Average Speed Control Segment
         */
        
        events

        .filter(new InsideControlSegment())

        .map(new ToAccSpeedFine())

        .keyBy(new AccSpeedFine.SelectorVID())

        .reduce(new MergeAccSpeedFine())

        .flatMap(new InCompleteSegment())

        .filter(new AboveAvgSpeedSegment())

        .writeAsCsv(args[1] + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE, "\n", ",");

        /**
         * Accidents
         */

        // events

        // .filter(new StopedVehicle())

        // .groupBy(new SelectorVID())

        // .sortGroup(new SelectorTime(), Order.ASCENDING)

        // .reduceGroup(new SamePositionEvent())

        // .flatMap(new AccidentVehicle())

        // .writeAsCsv(args[1] + "/accidents.csv", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute("VehicleTelematics");
        } catch(Exception e)  {
            e.printStackTrace();
        }

        // System.out.println(events.getParallelism());
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

    public static final class InCompleteSegment
    implements FlatMapFunction<AccSpeedFine,AvgSpeedFine> {
        @Override
        public void flatMap(AccSpeedFine e, Collector<AvgSpeedFine> out) {
            int[] all_segments = {52,53,54,55,56};

            boolean inSegment = true;

            for (int i = 0; i < all_segments.length && inSegment == true; i++) {
                if (!e.segments.contains(new Integer(all_segments[i]))) {
                    inSegment = false;
                }
            }

            if (inSegment) {
                int average = e.speed / e.n_event;
                out.collect(new AvgSpeedFine(e.time1,e.time2,e.vid,e.xWay,e.dir,average));
            }
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

    public static final class MergeAccSpeedFine
    implements ReduceFunction<AccSpeedFine> {
        @Override
        public AccSpeedFine reduce(AccSpeedFine e1, AccSpeedFine e2) {
            if (e1.time1 > e2.time1) { e1.time1 = e2.time1; } // Min time

            if (e1.time2 < e2.time2) { e1.time2 = e2.time2; } // Max time

            e1.speed += e2.speed; // Accumulated speed

            e1.n_event += 1; // Events counter

            e1.segments.addAll(e2.segments); // Segments visited

            return e1;
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
    implements KeySelector<Event, Long> {
        @Override
        public Long getKey(Event e) {
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

    public static class ToAccSpeedFine
    implements MapFunction<Event,AccSpeedFine> {
        @Override
        public AccSpeedFine map(Event e) {
            return new AccSpeedFine(e);
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

            long time_min = current.time;
            long time_max = current.time;
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