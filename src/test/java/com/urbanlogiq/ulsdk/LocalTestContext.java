// Copyright (c), CommunityLogiq Software

package com.urbanlogiq.ulsdk;

import java.util.HashMap;
import java.util.List;

// Tests supply the GET response. Reject all other requests.
public abstract class LocalTestContext extends RequestContext {
    @Override
    Region region() {
        return Region.CA;
    }

    @Override
    Environment environment() {
        return Environment.Stage;
    }

    @Override
    public byte[] post(String path, byte[] body, String mimetype, List<Pair<String, String>> params,
            HashMap<String, String> headers) {
        throw new AssertionError("Unexpected POST request.");
    }

    @Override
    public byte[] put(String path, byte[] body, String mimetype, List<Pair<String, String>> params,
            HashMap<String, String> headers) {
        throw new AssertionError("Unexpected PUT request.");
    }

    @Override
    public byte[] delete(
            String path, List<Pair<String, String>> params, HashMap<String, String> headers) {
        throw new AssertionError("Unexpected DELETE request.");
    }

    @Override
    public byte[] upload(String path, List<File> files) {
        throw new AssertionError("Unexpected file upload.");
    }
}
