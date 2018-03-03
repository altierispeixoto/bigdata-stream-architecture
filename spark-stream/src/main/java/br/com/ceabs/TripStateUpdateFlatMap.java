package br.com.ceabs;

import br.com.ceabs.domain.Event;
import br.com.ceabs.domain.TripInfo;
import br.com.ceabs.domain.TripUpdate;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 *
 * Created by altieris on 23/02/18.
 *
 */
public class TripStateUpdateFlatMap implements FlatMapGroupsWithStateFunction<String, Event, TripInfo, TripUpdate> {

    @Override
    public Iterator<TripUpdate> call(String deviceSerialNumber, Iterator<Event> events, GroupState<TripInfo> oldState) throws Exception {


        // If timed out, then remove trip and send final update
        if (oldState.hasTimedOut()) {

            List<TripUpdate> tripUpdateList = new ArrayList<>();
            TripUpdate finalUpdate = new TripUpdate(oldState.get().getTripId());

            finalUpdate.setDeviceSerialNumber(deviceSerialNumber);
            finalUpdate.setDurationMs(oldState.get().calculateDuration()); // TripDuration
            finalUpdate.setNumEvents(oldState.get().getNumEvents());

            finalUpdate.setSpeedMax(oldState.get().getSpeedMax());
            finalUpdate.setSpeedMin(oldState.get().getSpeedMin());

            finalUpdate.setSpeedAvg(oldState.get().calculateSpeedAvg());
            finalUpdate.setStartTrip(oldState.get().getEventTripStart());
            finalUpdate.setEndTrip(oldState.get().getEventTripEnd());
            finalUpdate.setTripStatus("FINISHED");
            finalUpdate.setExpired(true);
            oldState.remove();

            tripUpdateList.add(finalUpdate);

            return tripUpdateList.iterator();

        } else {


            long maxEventTime = Long.MIN_VALUE;
            long minEventTime = Long.MAX_VALUE;

            int numNewEvents = 0;
            double speedSum = 0;
            double speedMax = Double.MIN_VALUE;
            double speedMin = Double.MAX_VALUE;


            while (events.hasNext()) {
                Event e = events.next();

                long timestampEvent = e.getDtEvent().getTime();
                double speed = e.getSpeed();

                maxEventTime = Math.max(timestampEvent, maxEventTime);
                minEventTime = Math.min(timestampEvent, minEventTime);

                speedMax = Math.max(speed, speedMax);
                speedMin  = Math.min(speed, speedMin);

                speedSum += e.getSpeed();
                numNewEvents += 1;
            }



            TripInfo tripStaging = new TripInfo();


            // Update start and end timestamps in trip
            if (oldState.exists()) {

                TripInfo oldTrip = oldState.get();
                tripStaging.setTripId(oldTrip.getTripId());
                tripStaging.setNumEvents(oldTrip.getNumEvents() + numNewEvents);
                tripStaging.setSumSpeed(oldTrip.getSumSpeed() + speedSum);
                tripStaging.setSpeedMax(Math.max(oldTrip.getSpeedMax(),speedMax));
                tripStaging.setSpeedMin(Math.min(oldTrip.getSpeedMin(), speedMin));

                tripStaging.setEventTripStart(new Timestamp(Math.min(oldTrip.getEventTripStart().getTime(), minEventTime))); // Trip Start date
                tripStaging.setEventTripEnd(new Timestamp(Math.max(oldTrip.getEventTripEnd().getTime(), maxEventTime)));     // Trip End date


            } else {
                tripStaging.setTripId(String.valueOf(UUID.randomUUID()));
                tripStaging.setNumEvents(numNewEvents);
                tripStaging.setSumSpeed(speedSum);
                tripStaging.setEventTripStart(new Timestamp(minEventTime));
                tripStaging.setEventTripEnd(new Timestamp(maxEventTime));
                tripStaging.setSpeedMax(speedMax);
                tripStaging.setSpeedMin(speedMin);
            }


            oldState.update(tripStaging);

            // Set timeout such that the trip will be expired if no data received for 1 hour
            oldState.setTimeoutTimestamp(oldState.get().getEventTripEnd().getTime(), "2 minutes");

            List<TripUpdate> tripUpdateList = new ArrayList<>();

            TripUpdate tripUpdate = new TripUpdate(oldState.get().getTripId());
            tripUpdate.setDeviceSerialNumber(deviceSerialNumber);
            tripUpdate.setDurationMs(oldState.get().calculateDuration());
            tripUpdate.setNumEvents(oldState.get().getNumEvents());
            tripUpdate.setStartTrip(oldState.get().getEventTripStart());
            tripUpdate.setEndTrip(oldState.get().getEventTripEnd());

            tripUpdate.setSpeedMax(oldState.get().getSpeedMax());
            tripUpdate.setSpeedMin(oldState.get().getSpeedMin());

            tripUpdate.setSpeedAvg(oldState.get().calculateSpeedAvg());

            tripUpdate.setExpired(false);

            tripUpdate.setTripStatus("RUNNING");

            tripUpdateList.add(tripUpdate);

            return tripUpdateList.iterator();
        }
    }
}
