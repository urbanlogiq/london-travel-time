package com.urbanlogiq.traveltimeexample;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.JCommander;
import com.urbanlogiq.traveltimeexample.fbs.Job;
import com.urbanlogiq.traveltimeexample.fbs.Status;
import com.urbanlogiq.traveltimeexample.fbs.Task;

import java.io.FileReader;
import java.util.List;
import java.util.Map;

/**
 * This example will kick off a travel time job using the UrbanLogiq REST API with parameters that you specify
 * in a json file.  Once the job completes, it will write the results to a file in xlsx, csv of json format.
 *
 * Use "--params <filename.json>" or "-p <filename.json>" as a command line argument to specify the json file
 * containing the parameters.  If nothing is specify, then the program looks for "example_params.json" in the
 * program home directory (where a working example has been provided).
 *
 * Use "--format [xlsx/csv/json]" or "-f [xlsx/csv/json]" as a command line argument to specify the format to
 * write the results file in.  The results file will be named with a guid seeded from the worklog id (the unit of
 * work associated with the job that ran)
 */
public class TravelTimeExample {
    @Parameter(names={"--params", "-p"})
    private static String paramsFilename = "example_params.json";

    @Parameter(names={"--format", "-f"})
    private static String format = "xlsx";

    public static void main(String[] args) throws Exception {
        TravelTimeExample travelTimeExample = new TravelTimeExample();
        JCommander.newBuilder()
                .addObject(travelTimeExample)
                .build()
                .parse(args);

        UrbanLogiqAPIUtil ulApiUtil = new UrbanLogiqAPIUtil();
        Gson gson = new Gson();

        // Fill in example config.json file with your credentials and provided ids
        Map<String, String> config = null;
        try(JsonReader reader = new JsonReader(new FileReader("config.json"));) {
            config = gson.fromJson(reader, Map.class);
        }

        // ---- 1. Get access token for all UrbanLogiq requests -----
        // Get access token given client id provided and your urbanlogiq login email and password
        String token = ulApiUtil.getToken(config.get("client_id"), config.get("username"), config.get("password"));

        // ---- 2. Kick off a travel time job -----
        List<Map> params = null;
        try(JsonReader reader = new JsonReader(new FileReader(paramsFilename));) {
            Map<String, List> paramsJson = gson.fromJson(reader, Map.class);
            params = paramsJson.get("params");
        }
        // Build the runspec i.e. the instructions for running the job
        byte[] runspec = ulApiUtil.buildRunspec(config.get("schematic"), config.get("realm"), params);
        // Send the job to the schematic evaluator
        String jobId = ulApiUtil.postJob(token, runspec);

        // ---- 3. Wait for the job to complete ----
        Job job = ulApiUtil.getJob(token, jobId);
        while (job == null || (job.status() != Status.Complete && job.status() != Status.Error)) {
            job = ulApiUtil.getJob(token, jobId);
        }
        if (job.status() == Status.Error) {
            throw new UrbanLogiqAPIException("Job failed. Please contact UrbanLogiq.");
        }

        // ---- 4. Fetch the job results -----
        // There is a single task in the travel time job
        Task task = job.tasks(0);
        // A task produces a worklog, which groups all the output data together
        String worklogId = ulApiUtil.extractGuidFromObjectId(task.output());
        // Format argument can be any of the following: "csv", "xlsx", "json", it is "xlsx" by default
        // If format argument is missing from the request, data will be returned in arrow ipc format.
        String filename = ulApiUtil.writeResultsToFile(token, worklogId, format);
        System.out.println(String.format("Results written to %s", filename));
    }
}
