// Copyright (c), CommunityLogiq Software

package com.urbanlogiq.traveltimeexample;

import static org.junit.jupiter.api.Assertions.*;

import com.urbanlogiq.ulsdk.LocalTestContext;
import com.urbanlogiq.ulsdk.Pair;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class TravelTimeExampleTest {
    private static final long START = 1612569600000L;
    private static final long END = 1612652400000L;
    private static final int INTERVAL = 3600;

    @Test
    void requestUsesGetAndKeepsTheCsvBytes() throws Exception {
        for (String csv : List.of("", "description,travel_time",
                     "description,travel_time\r\n\"Café, North\",12\r\n")) {
            byte[] response = csv.getBytes(StandardCharsets.UTF_8);
            AtomicInteger calls = new AtomicInteger();
            LocalTestContext context = new LocalTestContext() {
                @Override
                public byte[] get(String path, List<Pair<String, String>> parameters,
                        HashMap<String, String> headers) {
                    calls.incrementAndGet();
                    assertEquals("/v1/api/ulv2/thirdparty/london/traveltime", path);
                    assertEquals(Map.of("accept", "text/csv"), headers);
                    assertEquals(3, parameters.size());
                    parameters.sort(Comparator.comparing(Pair::first));
                    assertEquals("end_time", parameters.get(0).first());
                    assertEquals(Long.toString(END), parameters.get(0).second());
                    assertEquals("interval", parameters.get(1).first());
                    assertEquals(Integer.toString(INTERVAL), parameters.get(1).second());
                    assertEquals("start_time", parameters.get(2).first());
                    assertEquals(Long.toString(START), parameters.get(2).second());
                    headers.put("x-ul-date", "test-date");
                    return response;
                }
            };
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            PrintStream previous = System.out;
            try (PrintStream stream = new PrintStream(output, false, StandardCharsets.US_ASCII)) {
                System.setOut(stream);
                TravelTimeExample.fetchTravelTime(context, START, END, INTERVAL);
            } finally {
                System.setOut(previous);
            }
            assertEquals(1, calls.get());
            assertArrayEquals(response, output.toByteArray());
        }
    }

    @Test
    void requestKeepsTheSelectedTimeRangeAndInterval() throws Exception {
        for (long[] values : new long[][] {{-1, 0, 1}, {START, START, 900},
                     {START, START + TimeUnit.DAYS.toMillis(366), Integer.MAX_VALUE}}) {
            long[] times = {values[0], values[1]};
            int interval = Math.toIntExact(values[2]);
            AtomicInteger calls = new AtomicInteger();
            LocalTestContext context = new LocalTestContext() {
                @Override
                public byte[] get(String path, List<Pair<String, String>> parameters,
                        HashMap<String, String> headers) {
                    calls.incrementAndGet();
                    assertEquals(3, parameters.size());
                    assertEquals("start_time", parameters.get(0).first());
                    assertEquals(Long.toString(times[0]), parameters.get(0).second());
                    assertEquals("end_time", parameters.get(1).first());
                    assertEquals(Long.toString(times[1]), parameters.get(1).second());
                    assertEquals("interval", parameters.get(2).first());
                    assertEquals(Integer.toString(interval), parameters.get(2).second());
                    return new byte[0];
                }
            };
            TravelTimeExample.fetchTravelTime(context, times[0], times[1], interval);
            assertEquals(1, calls.get());
        }
    }

    @Test
    void requestFailuresReachTheCallerWithoutOutput() {
        for (Exception failure : List.of(new IOException("Request failed."),
                     new InterruptedException("Request stopped."))) {
            AtomicInteger calls = new AtomicInteger();
            LocalTestContext context = new LocalTestContext() {
                @Override
                public byte[] get(String path, List<Pair<String, String>> parameters,
                        HashMap<String, String> headers) throws IOException, InterruptedException {
                    calls.incrementAndGet();
                    if (failure instanceof IOException io) {
                        throw io;
                    }
                    throw (InterruptedException) failure;
                }
            };
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            PrintStream previous = System.out;
            try (PrintStream stream = new PrintStream(output, false, StandardCharsets.UTF_8)) {
                System.setOut(stream);
                Executable request =
                        () -> TravelTimeExample.fetchTravelTime(context, START, END, INTERVAL);
                assertSame(failure, assertThrows(failure.getClass(), request));
            } finally {
                System.setOut(previous);
            }
            assertEquals(1, calls.get());
            assertEquals(0, output.size());
        }
    }
}
