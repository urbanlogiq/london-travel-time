package com.urbanlogiq.traveltimeexample;

import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import com.urbanlogiq.ulsdk.Key;
import com.urbanlogiq.ulsdk.Region;
import com.urbanlogiq.ulsdk.Environment;
import com.urbanlogiq.ulsdk.ApiKeyContext;
import com.urbanlogiq.ulsdk.api.schematicevaluator.SchematicEvaluator;
import com.urbanlogiq.ulsdk.api.datacatalog.Datacatalog;
import com.urbanlogiq.ulsdk.types.ObjectId;
import com.urbanlogiq.ulsdk.types.RunSpec;
import com.urbanlogiq.ulsdk.types.Job;
import com.urbanlogiq.ulsdk.types.EmbeddedTable;
import com.urbanlogiq.ulsdk.types.ParamIndices;
import com.urbanlogiq.ulsdk.types.Status;
import com.urbanlogiq.ulsdk.types.Task;
import com.urbanlogiq.ulsdk.types.TaskParameter;
import com.urbanlogiq.ulsdk.types.TaskParameterValue;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

public class TravelTimeExample {
    static class Parameter {
        String _realm;
        String _nodeId;
        long _startTime;
        long _endTime;
        int _interval;

        Parameter(String realm, String nodeId, long startTime, long endTime, int interval) {
            this._realm = realm;
            this._nodeId = nodeId;
            this._startTime = startTime;
            this._endTime = endTime;
            this._interval = interval;
        }
    }

    static byte[] writeArrowParameters(Parameter[] parameters) throws java.io.IOException {
        RootAllocator allocator = new RootAllocator();
        VarCharVector realms = new VarCharVector("realm", allocator);
        VarCharVector nodeIds = new VarCharVector("ul_node_id", allocator);
        TimeStampMilliVector startTimes = new TimeStampMilliVector("start_time", allocator);
        TimeStampMilliVector endTimes = new TimeStampMilliVector("end_time", allocator);
        IntVector intervals = new IntVector("interval", allocator);

        for (int i = 0; i < parameters.length; ++i) {
            Parameter p = parameters[i];
            realms.setSafe(i, p._realm.getBytes(StandardCharsets.UTF_8));
            nodeIds.setSafe(i, p._nodeId.getBytes(StandardCharsets.UTF_8));
            startTimes.setSafe(i, p._startTime);
            endTimes.setSafe(i, p._endTime);
            intervals.setSafe(i, p._interval);
        }

        int nparams = parameters.length;
        realms.setValueCount(nparams);
        nodeIds.setValueCount(nparams);
        startTimes.setValueCount(nparams);
        endTimes.setValueCount(nparams);
        intervals.setValueCount(nparams);

        List<Field> fields = Arrays.asList(realms.getField(), nodeIds.getField(), startTimes.getField(), endTimes.getField(), intervals.getField());
        List<FieldVector> vectors = Arrays.asList(realms, nodeIds, startTimes, endTimes, intervals);
        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, stream);

        writer.start();
        writer.writeBatch();
        writer.end();

        return stream.toByteArray();
    }

    static TaskParameter[] serializeParameters(Parameter[] parameters) throws java.io.IOException {
        byte[] arrowParameters = writeArrowParameters(parameters);

        EmbeddedTable parameterTable = new EmbeddedTable();
        parameterTable.setV(arrowParameters);

        TaskParameterValue value = new TaskParameterValue(parameterTable);
        TaskParameter taskParameter = new TaskParameter();
        taskParameter.setKey("params");
        taskParameter.setValue(value);

        return new TaskParameter[]{taskParameter};
    }

    static void fetchTravelTime(ApiKeyContext ctx, Parameter[] parameters) throws java.io.IOException, java.net.URISyntaxException, java.lang.InterruptedException {
        TaskParameter[] serializedParameters = serializeParameters(parameters);
        ParamIndices idx = new ParamIndices();
        idx.setIdxs(new int[]{ 0 });
        ParamIndices[] paramIndices = new ParamIndices[] { idx };

        RunSpec runSpec = new RunSpec();
        // This GUID is hardcoded for the travel time data processing job, please
        // do not change this.
        runSpec.setSchematic(new ObjectId("00004592-295a-f146-edf2-51b16ffb3252"));
        runSpec.setParams(serializedParameters);
        runSpec.setParamIndices(paramIndices);

        ObjectId id = SchematicEvaluator.createJob(
            ctx,
            runSpec
        );

        while (true) {
            Job job = SchematicEvaluator.getJob(ctx, id, null);
            byte status = job.getStatus();
            if (status == Status.Pending || status == Status.Running) {
                continue;
            }

            if (status == Status.Complete) {
                Task[] tasks = job.getTasks();
                Task travelTimeTask = tasks[0];
                ObjectId outputStream = travelTimeTask.getOutput();
                byte[] csv = Datacatalog.streamGetCsv(ctx, outputStream);

                // At this point you can do with the content what you wish, here
                // we just write it out to the terminal.
                System.out.println(new String(csv));
                break;
            }

            throw new RuntimeException("travel time job failed");
        }
    }

    public static void main(String[] args) throws java.io.IOException, java.net.URISyntaxException, java.lang.InterruptedException {
        // These are sample parameters which can be replaced with the necessary
        // parameters as required in order to retrieve your travel time results.
        Parameter[] sampleParameters = new Parameter[] {
            new Parameter("3dcf2d3e-cafd-4f79-b93d-ab09044a005d", "058kjDnuc+CDAfyiLei+Cw==", 1612569600000L, 1612652400000L, 3600),
            new Parameter("3dcf2d3e-cafd-4f79-b93d-ab09044a005d", "acGBlAS6mOixck/4YKSMwA==", 1612569600000L, 1612652400000L, 3600),
        };

        // Instead of using a username and password, the UrbanLogiq SDK uses signed
        // requests. These require an access key ID and a secret key, which can be
        // managed in the UrbanLogiq portal at https://home.urbanlogiq.ca/admin/keys
        UUID userId = UUID.fromString(System.getenv("USER_ID"));
        String accessKey = System.getenv("ACCESS_KEY");
        String secretKey = System.getenv("SECRET_KEY");

        Key key = new Key(userId, Region.CA, accessKey, secretKey);
        ApiKeyContext ctx = new ApiKeyContext(key, Environment.Prod);

        fetchTravelTime(ctx, sampleParameters);
    }
}
