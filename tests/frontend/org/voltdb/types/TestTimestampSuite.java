package org.voltdb.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.junit.Test;
import org.voltdb.common.Constants;

public class TestTimestampSuite {

    private static final long GREGORIAN_EPOCH = -12212553600000000L;  //  1583-01-01 00:00:00
    private static final long NYE9999          = 253402300799999999L; //  9999-12-31 23:59:59.999999

    private static final String MIN_STRING = "1583-01-01 00:00:00.000000";
    private static final String MAX_STRING = "9999-12-31 23:59:59.999999";
    private static final String UNIX_EPOCH = "1970-01-01 00:00:00.000000";

    void expectOutOfRangeException(long usecSinceEpoch) {
        boolean threw = false;
        try {
            new TimestampType(usecSinceEpoch);
        }
        catch (IllegalArgumentException iae) {
            threw = true;
            assertTrue(iae.getMessage().contains("timestamp value is outside of the supported range"));
        }
        assertTrue(threw);
    }

    @Test
    public void testLongConstructor() {
        assertEquals(MIN_STRING, new TimestampType(GREGORIAN_EPOCH).toStringGMT());
        assertEquals(UNIX_EPOCH, new TimestampType(0).toStringGMT());
        assertEquals(MAX_STRING, new TimestampType(NYE9999).toStringGMT());
        expectOutOfRangeException(GREGORIAN_EPOCH - 1);
        expectOutOfRangeException(NYE9999 + 1);
    }

    void expectOutOfRangeException(Date date) {
        boolean threw = false;
        try {
            new TimestampType(date);
        }
        catch (IllegalArgumentException iae) {
            threw = true;
            assertTrue(iae.getMessage().contains("timestamp value is outside of the supported range"));
        }
        assertTrue(threw);
    }

    @Test
    public void testDateConstructor() {
        assertEquals(GREGORIAN_EPOCH, new TimestampType(new Date(GREGORIAN_EPOCH / 1000)).getUSecSinceEpoch());
        assertEquals(0, new TimestampType(new Date(0)).getUSecSinceEpoch());
        // Java Date only has ms granularity.
        assertEquals(NYE9999 - 999, new TimestampType(new Date(NYE9999 / 1000)).getUSecSinceEpoch());

        expectOutOfRangeException(new Date(GREGORIAN_EPOCH / 1000 - 1));
        expectOutOfRangeException(new Date(NYE9999 / 1000 + 1));
    }

    // Take a String formatted as GMT and format it as the local time zone.
    private String gmtToLocal(String timestampStr) throws ParseException {
        SimpleDateFormat gmtFormatter = new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
        gmtFormatter.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
        Date d = gmtFormatter.parse(timestampStr);

        SimpleDateFormat localFormatter = new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
        return localFormatter.format(d);
    }

    private void expectException(String dateStr, String expectedMsg) throws ParseException {
        boolean threw = false;
        try {
            new TimestampType(gmtToLocal(dateStr));
        }
        catch (IllegalArgumentException iae) {
            threw = true;
            String actualMsg = iae.getMessage();
            assertTrue("expected: " + expectedMsg + ", actual: " + actualMsg,
                        actualMsg.contains(expectedMsg));
        }
        assertTrue(threw);
    }

    // Difference from UTC in milliseconds
    private int localTimeZoneOffset() {
        SimpleDateFormat sdf = new SimpleDateFormat();
        int rawOffset = sdf.getTimeZone().getRawOffset();
        return rawOffset;
    }

    @Test
    public void testStringConstructor() throws ParseException {
        // For some reason, timestamp strings with microsecond granularity
        // messes up Java's date formatting.
        String maxStrMillis = "9999-12-31 23:59:59.999";

        assertEquals(GREGORIAN_EPOCH, new TimestampType(gmtToLocal(MIN_STRING)).getUSecSinceEpoch());
        assertEquals(0, new TimestampType(gmtToLocal(UNIX_EPOCH)).getUSecSinceEpoch());

        if (localTimeZoneOffset() <= 0) {
            assertEquals(NYE9999 - 999, new TimestampType(gmtToLocal(maxStrMillis)).getUSecSinceEpoch());
        }
        else {
            // Converting upper bound to a local time zone when East of UTC
            // would create a 5-digit year, which Java date string conversion cannot handle.
            expectException("10000-01-01 00:00:00.000", "Timestamp format must be yyyy-mm-dd");
        }

        expectException("1582-12-31 23:59:59.999", "timestamp value is outside of the supported range");

        if (localTimeZoneOffset() <= 0) {
            // East of UTC we'll see the formatting exception that we got above.
            expectException("10000-01-01 00:00:00.000", "timestamp value is outside of the supported range");
        }

    }
}
