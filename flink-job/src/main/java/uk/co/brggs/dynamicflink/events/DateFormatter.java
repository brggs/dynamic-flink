package uk.co.brggs.dynamicflink.events;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Holds a single instance of the date format used for reading/writing dates.
 */
public class DateFormatter {
    private static SimpleDateFormat format = null;

    public static SimpleDateFormat getInstance() {
        if (format == null) {
            format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
        }
        return format;
    }
}
