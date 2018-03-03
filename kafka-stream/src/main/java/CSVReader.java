import com.google.common.base.Charsets;
import com.google.common.io.Resources;


import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created by altieris on 22/02/18.
 *
 */
public class CSVReader {


    public static String readEventCsv () {

        try {
         return readResource("event.csv", Charsets.UTF_8);

        }catch (IOException ioex){
            ioex.printStackTrace();
        }
        return "";
    }

    private static String readResource(final String fileName, Charset charset) throws IOException {
        return Resources.toString(Resources.getResource(fileName), charset);
    }

}
