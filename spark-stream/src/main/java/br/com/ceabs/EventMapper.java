package br.com.ceabs;

import br.com.ceabs.domain.Event;
import br.com.ceabs.domain.LineWithTimestamp;
import org.apache.spark.api.java.function.MapFunction;

public class EventMapper implements MapFunction<LineWithTimestamp, Event>{
    @Override
    public Event call(LineWithTimestamp lineWithTimestamp) {
        return new Event(lineWithTimestamp);
    }
}
