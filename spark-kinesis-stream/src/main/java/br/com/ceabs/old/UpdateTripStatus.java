package br.com.ceabs.old;

import br.com.ceabs.domain.Event;
import br.com.ceabs.domain.Trip;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;

import java.util.List;

/**
 *
 * Created by altieris on 19/02/18.
 *
 */
public class UpdateTripStatus implements Function2<List<List<Event>>, Optional<Trip>, Optional<Trip>> {


    @Override
    public Optional<Trip> call(List<List<Event>> v1, Optional<Trip> v2) throws Exception {

        Trip trip;

        if (v1.size() == 0) {
            if (v2.isPresent()) {
                return v2;
            }
        } else {

            List<Event> events = v1.get(0);

            if (v2.isPresent()) {
                trip = v2.get();
            } else {
                trip = new Trip();
                trip.setDeviceSerialNumber(events.get(0).getDeviceSerialNumber());
            }

            for (Event evt : events) {
                if (evt.getSpeed() > trip.getSpeedMax()) {

//                    trip.setSpeedMax(evt.getSpeed());
//                    if(evt.getSpeed() > 178 && !evt.getDeviceSerialNumber().equals("5")){
//                        trip.setEndTrip(true);
//                    }
                }
            }

            return Optional.of(trip);
        }
        return null;
    }

}
