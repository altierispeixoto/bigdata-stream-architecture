/*
package br.com.ceabs;

import br.com.ceabs.domain.Event;
import br.com.ceabs.domain.TripInfo;
import br.com.ceabs.domain.TripUpdate;

import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;

import java.util.Iterator;


// Turn events into trips. Track number of events, start and end timestamps of session, and
// and report session updates.
//
// Step 1: Define the state update function
public class TripStateUpdate implements MapGroupsWithStateFunction<String, Event, TripInfo, TripUpdate> {

    @Override
    public TripUpdate call(String deviceSerialNumber, Iterator<Event> events, GroupState<TripInfo> oldState){

    */
/*    // If timed out, then remove trip and send final update
        if (oldState.hasTimedOut()) {

            TripUpdate finalUpdate = new TripUpdate();

            finalUpdate.setDeviceSerialNumber(deviceSerialNumber);
            finalUpdate.setDurationMs(oldState.get().calculateDuration());
            finalUpdate.setNumEvents(oldState.get().getNumEvents());

            SummaryStatistics statistics = new SummaryStatistics();
            for (Double speed : oldState.get().getSpeedList()){
                statistics.addValue(speed);
            }

            finalUpdate.setSpeedMax(statistics.getMax());
            finalUpdate.setSpeedMin(statistics.getMin());
            finalUpdate.setSpeedAvg(statistics.getMean());
            *//*
*/
/*finalUpdate.setStartTrip(new Timestamp(oldState.get().getStartTimestampMs()));
            finalUpdate.setEndTrip(new Timestamp(oldState.get().getEndTimestampMs()));*//*
*/
/*
            finalUpdate.setExpired(true);
            oldState.remove();

            return finalUpdate;

        } else {

            // Find max and min timestamps in events
            long maxTimestampMs = Long.MIN_VALUE;
            long minTimestampMs = Long.MAX_VALUE;
            int numNewEvents = 0;


            ArrayList<Double> speedList = new ArrayList<>();

            while (events.hasNext()) {
                Event e = events.next();
                long timestampMs = e.getTimestamp().getTime();
                maxTimestampMs = Math.max(timestampMs, maxTimestampMs);
                minTimestampMs = Math.min(timestampMs, minTimestampMs);

                speedList.add(e.getSpeed());
                numNewEvents += 1;
            }

            TripInfo updatedTrip = new TripInfo();

            // Update start and end timestamps in trip
            if (oldState.exists()) {

                TripInfo oldTrip = oldState.get();
                updatedTrip.setNumEvents(oldTrip.getNumEvents() + numNewEvents);
                updatedTrip.setStartTimestampMs(oldTrip.getStartTimestampMs());
                updatedTrip.setEndTimestampMs(Math.max(oldTrip.getEndTimestampMs(), maxTimestampMs));
                updatedTrip.setSpeedList(oldTrip.getSpeedList());

            } else {

                updatedTrip.setNumEvents(numNewEvents);
                updatedTrip.setStartTimestampMs(minTimestampMs);
                updatedTrip.setEndTimestampMs(maxTimestampMs);
            }

            if(speedList.size() > 0) {
                for (Double speed : speedList) {
                    updatedTrip.getSpeedList().add(speed);
                }
            }


            oldState.update(updatedTrip);

            // Set timeout such that the trip will be expired if no data received for 1 hour
            oldState.setTimeoutTimestamp(maxTimestampMs,"1 hour");

            TripUpdate tripUpdate = new TripUpdate();
            tripUpdate.setDeviceSerialNumber(deviceSerialNumber);
            tripUpdate.setDurationMs(oldState.get().calculateDuration());
            tripUpdate.setNumEvents(oldState.get().getNumEvents());
//            tripUpdate.setStartTrip(new Timestamp(oldState.get().getStartTimestampMs()));
//            tripUpdate.setEndTrip(new Timestamp(oldState.get().getEndTimestampMs()));

            SummaryStatistics statistics = new SummaryStatistics();
            for (Double speed : oldState.get().getSpeedList()){
                statistics.addValue(speed);
            }

            tripUpdate.setSpeedMax(statistics.getMax());
            tripUpdate.setSpeedMin(statistics.getMin());
            tripUpdate.setSpeedAvg(statistics.getMean());

            tripUpdate.setExpired(false);

            return tripUpdate;
            }
            *//*

        return null;

    }
}
*/
