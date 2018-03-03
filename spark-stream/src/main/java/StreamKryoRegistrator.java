import com.esotericsoftware.kryo.Kryo;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import lombok.NonNull;
import org.apache.spark.serializer.KryoRegistrator;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.lang.reflect.Array;

/**
 *
 * Created by altieris on 21/12/17.
 *
 */
public class StreamKryoRegistrator implements KryoRegistrator, Serializable {


    public StreamKryoRegistrator() {}

    protected void doRegistration(@Nonnull Kryo kryo, @Nonnull String s ) {
        Class c;
        try {
            c = Class.forName(s);
            doRegistration(kryo,  c);
        }
        catch (ClassNotFoundException e) {
            return;
        }
    }


    protected void doRegistration(@NonNull final Kryo kryo ,@NonNull final Class pC) {
        if (kryo != null) {
            kryo.register(pC);
            // also register arrays of that class
            Class arrayType = Array.newInstance(pC, 0).getClass();
            kryo.register(arrayType);
        }
    }



    @Override
    public void registerClasses(@Nonnull Kryo kryo) {
        registerBasic(kryo);
        kryo.register( DateTime.class, new JodaDateTimeSerializer() );

    }


    private Kryo registerBasic(@Nonnull Kryo kryo){
        kryo.register(Object[].class);
        kryo.register(scala.Tuple2[].class);
        kryo.register(int[].class);
        kryo.register(Class.class);
        kryo.register(Object.class);
        kryo.register(java.util.Date.class);
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(String.class);
        return kryo;
    }

}