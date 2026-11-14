// Copyright (c), CommunityLogiq Software

package com.urbanlogiq.traveltimeexample;

import com.urbanlogiq.ulsdk.ApiKeyContext;
import com.urbanlogiq.ulsdk.Environment;
import com.urbanlogiq.ulsdk.Key;
import com.urbanlogiq.ulsdk.Pair;
import com.urbanlogiq.ulsdk.Region;
import com.urbanlogiq.ulsdk.RequestContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public final class TravelTimeExample {
    static final String TRAVEL_TIME_PATH = "/v1/api/ulv2/thirdparty/london/traveltime";

    static void fetchTravelTime(RequestContext context, long startTime, long endTime, int interval)
            throws IOException, URISyntaxException, InterruptedException {
        // The SDK sorts these parameters and adds request headers.
        List<Pair<String, String>> parameters = new ArrayList<>();
        parameters.add(new Pair<>("start_time", Long.toString(startTime)));
        parameters.add(new Pair<>("end_time", Long.toString(endTime)));
        parameters.add(new Pair<>("interval", Integer.toString(interval)));
        HashMap<String, String> headers = new HashMap<>();
        headers.put("accept", "text/csv");

        byte[] csv = context.get(TRAVEL_TIME_PATH, parameters, headers);
        System.out.write(csv);
    }

    public static void main(String[] args)
            throws IOException, URISyntaxException, InterruptedException {
        // Change these sample values. Both ends of the time range are included.
        // Times are milliseconds from 1970-01-01 00:00:00 UTC.
        long sampleStartTime = 1612569600000L;
        long sampleEndTime = 1612652400000L;
        // The interval is in seconds. Each row is the average over one interval.
        int sampleInterval = 3600;

        // Set USER_ID, ACCESS_KEY, and SECRET_KEY before you run this program.
        UUID userId = UUID.fromString(System.getenv("USER_ID"));
        String accessKey = System.getenv("ACCESS_KEY");
        String secretKey = System.getenv("SECRET_KEY");

        Key key = new Key(userId, Region.CA, accessKey, secretKey);
        ApiKeyContext context = new ApiKeyContext(key, Environment.Prod);
        fetchTravelTime(context, sampleStartTime, sampleEndTime, sampleInterval);
    }
}
