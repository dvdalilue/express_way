package master2017.flink;

import java.util.Iterator;
import java.util.ArrayList;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Implements the "VehicleTelematics" program that analize the vehicles speed on
 * a certain express way with a record of events
 */
public class VehicleTelematics {
    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> events =
            env.readTextFile(args[0]).setParallelism(1).map(new ToEvent());

        /**
         * Speed Radar
         */
        
        events

        .flatMap(new SpeedRadar())

        .writeAsCsv(args[1] + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE, "\n", ",");
        
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

        final int windowSize = 4;

        events

        .filter(new StopedVehicle())

        .keyBy(new Event.SelectorVID())

        .countWindow(windowSize,1)

        .apply(new AccidentVehicle(windowSize))

        .writeAsCsv(args[1] + "/accidents.csv", FileSystem.WriteMode.OVERWRITE, "\n", ",");

        try {
            env.execute("VehicleTelematics");
        } catch(Exception e)  {
            e.printStackTrace();
        }
    }

    /////////////////////
    //  Map Functions  //
    /////////////////////

    /**
     * Implements a map function to transform a String from a CSV file (a line),
     * into an Event instance.
     */
    public static final class ToEvent
    implements MapFunction<String, Event> {
        @Override
        public Event map(String in) {
            String[] s = in.split(",");

            return new 
                Event(
                    Integer.parseInt(s[0]),
                    Integer.parseInt(s[1]),
                    Integer.parseInt(s[2]),
                    Integer.parseInt(s[3]),
                    Integer.parseInt(s[4]),
                    Integer.parseInt(s[5]),
                    Integer.parseInt(s[6]),
                    Integer.parseInt(s[7])
                )
            ;
        }
    }

    /**
     * Implements a map function to convert an Event instance into a SpeedFine
     * accumulator. It's used to get the average speed within a segment.
     */
    public static final class ToAccSpeedFine
    implements MapFunction<Event,AccSpeedFine> {
        @Override
        public AccSpeedFine map(Event e) {
            return new AccSpeedFine(e);
        }
    }

    //////////////////////////
    //  Flat Map Functions  //
    //////////////////////////

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
     * Implements a function to collect every AccSpeedFine within a specific
     * segment, omiting those outside of it. The AccSpeedFine instance is an
     * accumulator. It has a field with the accumulated segments, which is used
     * to check if the measure is valid.
     */
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
                out.collect(new AvgSpeedFine(e.min,e.max,e.vid,e.xWay,e.dir,average));
            }
        }
    }

    ////////////////////////
    //  Filter Functions  //
    ////////////////////////

    /**
     * Implements a filter function to discard events outside the control
     * segment of the express way. Leaving the dataStream only events with the
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

    /**
     * Implements the filter function to get the events with the zero (0) speed.
     * It's used to search for accidented vehicles.
     */
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

    ////////////////////////
    //  Reduce Functions  //
    ////////////////////////

    /**
     * Implements a reduce function to fold the accumulators events into a
     * unique instance, which is filtered after by a specific criteria. Asigning
     * the minimum time in 'min', the maximum time in 'max', the total speed
     * in 'speed', the number of reduced events in 'n_event' and keeping the 
     * visited segments by the vehicle in an ArrayList<Integer>.
     */
    public static final class MergeAccSpeedFine
    implements ReduceFunction<AccSpeedFine> {
        @Override
        public AccSpeedFine reduce(AccSpeedFine e1, AccSpeedFine e2) {
            if (e1.min > e2.min) { e1.min = e2.min; } // Min time

            if (e1.max < e2.max) { e1.max = e2.max; } // Max time

            e1.speed += e2.speed; // Accumulated speed

            e1.n_event += e2.n_event; // Events counter

            e1.segments.addAll(e2.segments); // Segments visited

            return e1;
        }
    }

    ////////////////////////
    //  Window Functions  //
    ////////////////////////

    /**
     * Implements a window function with IN values of class Event, OUT values of
     * class Accident, KEY values by vid and with tumbling windows. The main
     * propuse of this class/method is to check if the events in the window have
     * the same position. Also, the amount of events must be exactly four (4).
     * It's important to keep the minimum time (begining) and maximum time (end).
     */
    public static final class AccidentVehicle
    implements WindowFunction<Event, Accident, Integer, GlobalWindow> {
        private int window_size;

        public AccidentVehicle(int w) {
            this.window_size = w;
        }

        @Override
        public void apply(Integer key, GlobalWindow window, Iterable<Event> events, Collector<Accident> out) {

            Iterator<Event> it = events.iterator();

            Event e1 = it.next();
            Event e2;

            long begin_time = e1.time;
            long end_time = e1.time;

            boolean same_pos = true;
            int n = 1;

            while (it.hasNext()) {
                e2 = it.next();

                if (e1.pos != e2.pos) { same_pos = false; break; }

                if (begin_time > e2.time) { begin_time = e2.time; }
                if (end_time < e2.time) { end_time = e2.time; }

                n++;
                e1 = e2;
            }

            if (same_pos && n == this.window_size) {
                out.collect(new Accident(begin_time,end_time,e1.vid,e1.xWay,e1.seg,e1.dir,e1.pos));
            }
        }
    }
}