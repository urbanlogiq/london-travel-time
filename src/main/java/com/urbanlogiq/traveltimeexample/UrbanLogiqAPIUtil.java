package com.urbanlogiq.traveltimeexample;

import com.urbanlogiq.traveltimeexample.fbs.*;

import com.google.gson.Gson;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.ByteBufferUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.net.URLEncoder;

/**
 * Utilities for interacting with the UrbanLogiq API, specifically written for the Travel Time Example.
 */
public class UrbanLogiqAPIUtil {
    public UrbanLogiqAPIUtil() { }

    private enum SupportedHttpMethods { GET, POST };

    /**
     * Builds a runspec, i.e. the instructions for running a job using the UL API
     * @param schematicId ID of the schematic object associated with the travel time job
     * @param realm ID for the realm used to identify worldgraph node(s) associated with targeted region
     * @param params Parameters to apply to the job
     * @return data about the job in byte array form
     * @throws UrbanLogiqAPIException
     * @throws IOException
     */
    public byte[] buildRunspec(String schematicId, String realm, List<Map> params) throws UrbanLogiqAPIException, IOException {
        // For documentation on FlatBuffers, see https://google.github.io/flatbuffers/index.html
        FlatBufferBuilder builder = new FlatBufferBuilder(0);
        int schematicOffset = writeId(builder, schematicId);
        int paramIndex = 0;
        ParamIndices.startIdxsVector(builder, 1);
        builder.addInt(paramIndex);
        int idxsOffset = builder.endVector();
        int paramIndexOffset = ParamIndices.createParamIndices(builder, idxsOffset);
        RunSpec.startParamIndicesVector(builder, 1);
        builder.addOffset(paramIndexOffset);
        int paramIndicesVectorOffset = builder.endVector();

        // For documentation on Apache Arrow, see https://arrow.apache.org/docs/java/
        // Apache Arrow vector initialization
        int numParams = params.size();
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VarCharVector realmVector = new VarCharVector("realm", allocator);
        realmVector.allocateNew(numParams);
        VarCharVector ulNodeIdVector = new VarCharVector("ul_node_id", allocator);
        ulNodeIdVector.allocateNew(numParams);
        TimeStampMilliVector startTimeVector = new TimeStampMilliVector("start_time", allocator);
        startTimeVector.allocateNew(numParams);
        TimeStampMilliVector endTimeVector = new TimeStampMilliVector("end_time", allocator);
        endTimeVector.allocateNew(numParams);
        IntVector intervalVector = new IntVector("interval", allocator);
        intervalVector.allocateNew(numParams);

        // Populate the Arrow vectors with what's in the params
        for (int i = 0; i < params.size(); i++) {
            Map param = params.get(i);
            realmVector.set(i, new Text(realm));
            ulNodeIdVector.set(i, new Text((String)param.get("ul_node_id")));
            startTimeVector.set(i, (long)(double)param.get("start_time"));
            endTimeVector.set(i, (long)(double)param.get("end_time"));
            intervalVector.set(i, (int)(double)param.get("interval"));
        }

        realmVector.setValueCount(numParams);
        ulNodeIdVector.setValueCount(numParams);
        startTimeVector.setValueCount(numParams);
        endTimeVector.setValueCount(numParams);
        intervalVector.setValueCount(numParams);

        List<FieldVector> columns = Arrays.asList(new FieldVector[]{
                realmVector,
                ulNodeIdVector,
                startTimeVector,
                endTimeVector,
                intervalVector
        });

        try {
            int paramOffset = writeTaskParameter(builder, columns);
            RunSpec.startParamIndicesVector(builder, 1);
            builder.addOffset(paramOffset);
            int paramsOffset = builder.endVector();

            int runspec = RunSpec.createRunSpec(builder, false, schematicOffset, paramIndicesVectorOffset, paramsOffset);
            builder.finishSizePrefixed(runspec);
        } finally {
            realmVector.close();
            ulNodeIdVector.close();
            startTimeVector.close();
            endTimeVector.close();
            intervalVector.close();
        }

        return builder.sizedByteArray();
    }

    /**
     * Gets GUID from an ObjectId, often used in UL API requests relating to the object
     * @param objectId FlatBuffer ObjectID object to extract the GUID from
     * @return GUID for the ObjectID in String format
     */
    public String extractGuidFromObjectId(ObjectId objectId) {
        List<Integer> intList = new ArrayList<Integer>();
        for (int i = 0; i < objectId.bLength(); i++) {
            intList.add(objectId.b(i));
        }
        String hexes = getPaddedHexStr(intList);
        String guid = hexes.substring(0, 8)
                + "-" + hexes.substring(8, 12)
                + "-" + hexes.substring(12, 16)
                + "-" + hexes.substring(16, 20)
                + "-" + (hexes.substring(20));
        return guid.toLowerCase();
    }

    /**
     * Returns a filename based on a ContentDisposition response header in String format
     * @param contentDisposition ContentDisposition header as a String
     * @return filename embedded within the header
     */
    private String getFilename(String contentDisposition) {
        String filename = contentDisposition.split("=")[1];
        filename = StringUtils.strip(filename, "\"");
        return filename;
    }

    /**
     * Uses the UrbanLogiq API to get information about a schematic evaluator job (with the given jobid), and return
     * this data as a FlatBuffer Job object.
     * @param token Credential token used to make a request to the UL API
     * @param jobId ID of the schematic evaluator job
     * @return Job object containing data about the job identified by the job id
     * @throws IOException
     * @throws UrbanLogiqAPIException
     */
    public Job getJob(String token, String jobId) throws IOException, UrbanLogiqAPIException {
        Job job = null;
        String endpoint = String.format("https://api.urbanlogiq.ca/v1/api/ulv2/schematicevaluator/jobs/%s", jobId);
        List<Header> headers = getHeaders(token, "application/octet-stream");

        try {
            byte[] jobBytes = sendHttpGetBytes(headers, endpoint, SupportedHttpMethods.GET, null);

            if (jobBytes != null) {
                ByteBuffer withoutSizePrefix = ByteBufferUtil.removeSizePrefix(ByteBuffer.wrap(jobBytes));
                job = Job.getRootAsJob(withoutSizePrefix);
            } else {
                throw new UrbanLogiqAPIException("HTTP entity was empty");
            }
        } catch (Exception e) {
            throw e;
        }

        return job;
    }

    /**
     * Gets a token that can be used for authentication with the UrbanLogiq API
     * @param clientId Client ID used to authenticate with Azure Active Directory
     * @param username UrbanLogiq username of the user
     * @param password UrbanLogiq password of the user
     * @return token that can be used for authentication with the UrbanLogiq API for a limited time
     * @throws UrbanLogiqAPIException
     * @throws IOException
     */
    public String getToken(String clientId, String username, String password) throws UrbanLogiqAPIException, IOException {
        String token = null;

        String tokenBaseEndpoint = "https://urbanlogiqcanada.b2clogin.com/urbanlogiqcanada.onmicrosoft.com/oauth2/v2.0/token?";
        String endpoint = String.format(
                "%sp=B2C_1_ropc&grant_type=password&response_type=token&client_id=%s&username=%s&password=%s&scope=openid+%s",
                tokenBaseEndpoint,
                clientId,
                username,
                URLEncoder.encode(password, StandardCharsets.UTF_8),
                clientId
        );

        String responseString = sendHttpGetString(null, endpoint, SupportedHttpMethods.POST, null);

        Gson jsonResult = new Gson();
        Map<String, String> responseMap = jsonResult.fromJson(responseString, Map.class);
        if (responseMap.get("access_token") != null) {
            token = responseMap.get("access_token");
        } else {
            throw new UrbanLogiqAPIException("Could not get status token");
        }

        return token;
    }

    /**
     * Use a runspec to create a new schematic evaluator job using the UrbanLogiq API
     * @param token valid token for authenticating with the UrbanLogiq API
     * @param runspec runspec that defines the job to be run in Apache Arrow IPC format
     * @return GUID of the schematic evaluator job that was run
     * @throws IOException
     * @throws UrbanLogiqAPIException
     */
    public String postJob(String token, byte[] runspec) throws IOException, UrbanLogiqAPIException {
        String guid = null;
        String endpoint = "https://api.urbanlogiq.ca/v1/api/ulv2/schematicevaluator/jobs";
        List<Header> headers = getHeaders(token, "application/octet-stream");
        byte[] jobIdBytes = sendHttpGetBytes(headers, endpoint, SupportedHttpMethods.POST, runspec);

        ByteBuffer withoutSizePrefix = ByteBufferUtil.removeSizePrefix(ByteBuffer.wrap(jobIdBytes));
        ObjectId objectId = ObjectId.getRootAsObjectId(withoutSizePrefix);
        guid = extractGuidFromObjectId(objectId);

        return guid;
    }

    /**
     * Given a worklog ID (i.e. results of a schematic evaluator job that has run), writes the results to a file
     * @param token valid token for authenticating with the UrbanLogiq API
     * @param worklogId worklog ID to extract results from and write to file
     * @param format format of output file, can be xlsx, csv or json
     * @throws IOException
     * @throws UrbanLogiqAPIException
     */
    public String writeResultsToFile(String token, String worklogId, String format) throws IOException, UrbanLogiqAPIException {
        String filename = null;
        String endpoint = String.format("https://api.urbanlogiq.ca/v1/api/ulv2/datacatalog/stream/%s", worklogId);
        if (format != null) {
            endpoint += String.format("?format=%s", format);
        }
        List<Header> headers = getHeaders(token,"application/octet-stream");
        HttpGet get = new HttpGet(endpoint);
        for (Header header : headers) {
            get.addHeader(header);
        }

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse resultsResponse = httpClient.execute(get)) {
            HttpEntity resultsEntity = resultsResponse.getEntity();

            if (resultsEntity != null) {
                filename = getFilename(resultsResponse.getFirstHeader("content-disposition").getValue());
                writeBytesToFile(EntityUtils.toByteArray(resultsEntity), filename);
                EntityUtils.consume(resultsEntity);
            } else {
                throw new UrbanLogiqAPIException("HTTP entity was empty");
            }
        }

        return filename;
    }

    /**
     * Gets the standard headers used for requests to the UrbanLogiq API
     * @param token valid token for authenticating with the UrbanLogiq API
     * @param contentType value to use for content-type header
     * @return List of header objects that can be attached to UL API requests
     */
    private List<Header> getHeaders(String token, String contentType) {
        String authorization = "Bearer " + token;
        List<Header> headers = new ArrayList<Header>();
        headers.add(new BasicHeader("authority", "api.urbanlogiq.ca"));
        headers.add(new BasicHeader("authorization", authorization));
        headers.add(new BasicHeader("content-type", contentType));

        return headers;
    }

    /**
     * Gets a padded hex string representation of a list of integers
     * @param intList list of integers
     * @return padded hex string representation of the integers
     */
    private String getPaddedHexStr(List<Integer> intList) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i : intList) {
            stringBuilder.append(String.format("%02X", i));
        }
        return stringBuilder.toString();
    }

    /**
     * Send an HTTP request and returns the response object
     * @param headers list of headers to attach to the request, can be null
     * @param endpoint URI endpoint to send the request to
     * @param httpMethod HTTP method supported by this class (hence the enum format)
     * @param payload payload to send in byte array format, if any (can be null)
     * @return a closeable http response object (cleanup should be handled by the caller)
     * @throws UrbanLogiqAPIException
     * @throws IOException
     */
    private String sendHttpGetString(List<Header> headers, String endpoint, SupportedHttpMethods httpMethod, byte[] payload)
            throws UrbanLogiqAPIException, IOException {
        HttpRequestBase request = null;
        if (httpMethod == SupportedHttpMethods.POST) {
            request = new HttpPost(endpoint);
        } else if (httpMethod == SupportedHttpMethods.GET) {
            request = new HttpGet(endpoint);
        } else {
            // We should never get here but let's handle it just in case
            throw new UrbanLogiqAPIException(String.format("HTTP method %s is not support by UrbanLogiqAPIUtil", httpMethod));
        }

        if (headers != null) {
            for (Header header : headers) {
                request.addHeader(header);
            }
        }

        if (httpMethod == SupportedHttpMethods.POST && payload != null && payload.length > 0) {
            ((HttpPost)request).setEntity(new ByteArrayEntity(payload));
        }


        try(CloseableHttpClient httpClient = HttpClients.custom().disableContentCompression().disableConnectionState().build()) {
            try(CloseableHttpResponse response = httpClient.execute(request)) {
                int statusCode = response.getStatusLine().getStatusCode();
                HttpEntity responseEntity = response.getEntity();

                String responseString = null;
                if (responseEntity != null) {
                    responseString = EntityUtils.toString(responseEntity);
                    EntityUtils.consume(responseEntity);
                }

                if (statusCode < 200 || statusCode >= 400) {
                    String reasonPhrase = response.getStatusLine().getReasonPhrase();
                    throw new UrbanLogiqAPIException(String.format("Status code is %s\nStatus message is %s\nresponse string: %s\n", statusCode, reasonPhrase, responseString));
                }

                return responseString;
            }
        }
    }

    private byte[] sendHttpGetBytes(List<Header> headers, String endpoint, SupportedHttpMethods httpMethod, byte[] payload)
            throws UrbanLogiqAPIException, IOException {
        HttpRequestBase request = null;
        if (httpMethod == SupportedHttpMethods.POST) {
            request = new HttpPost(endpoint);
        } else if (httpMethod == SupportedHttpMethods.GET) {
            request = new HttpGet(endpoint);
        } else {
            // We should never get here but let's handle it just in case
            throw new UrbanLogiqAPIException(String.format("HTTP method %s is not support by UrbanLogiqAPIUtil", httpMethod));
        }

        if (headers != null) {
            for (Header header : headers) {
                request.addHeader(header);
            }
        }

        if (httpMethod == SupportedHttpMethods.POST && payload != null && payload.length > 0) {
            ((HttpPost)request).setEntity(new ByteArrayEntity(payload));
        }


        try(CloseableHttpClient httpClient = HttpClients.custom().disableContentCompression().disableConnectionState().build()) {
            try(CloseableHttpResponse response = httpClient.execute(request)) {
                int statusCode = response.getStatusLine().getStatusCode();
                HttpEntity responseEntity = response.getEntity();

                byte[] bytes = null;
                if (responseEntity != null) {
                    bytes = EntityUtils.toByteArray(responseEntity);
                    EntityUtils.consume(responseEntity);
                }

                if (statusCode < 200 || statusCode >= 400) {
                    String reasonPhrase = response.getStatusLine().getReasonPhrase();
                    throw new UrbanLogiqAPIException(String.format("Status code is %s\nStatus message is %s\n", statusCode, reasonPhrase));
                }

                return bytes;
            }
        }
    }

    /**
     * Writes given bytes out to a file with the given filename
     * @param bytes data to write to file
     * @param filename filename to write data to
     * @throws IOException
     */
    private void writeBytesToFile(byte[] bytes, String filename) throws IOException {
        File file = new File(filename);
        file.createNewFile();

        try (FileOutputStream fos = new FileOutputStream(filename)) {
            fos.write(bytes);
        }
    }

    /**
     * Write ObjectID to FlatBuffer builder
     * @param builder FlatBuffer builder
     * @param id ID string to create the ObjectID from
     * @return offset in the FlatBufferBuilder after the write operation
     * @throws UrbanLogiqAPIException
     */
    private int writeId(FlatBufferBuilder builder, String id) throws UrbanLogiqAPIException {
        byte[] oid = UUIDToBytes(UUID.fromString(id));
        if (oid.length != 16) {
            throw new UrbanLogiqAPIException("Unexpected ID length: " + oid.length);
        }
        int idOffset = builder.createByteVector(oid);
        ObjectId.startObjectId(builder);
        ObjectId.addB(builder, idOffset);
        return ObjectId.endObjectId(builder);
    }

    /**
     * Write out Apache Arrow vectors to IPC format
     * @param columns a list of FieldVector objects
     * @return byte array containing vector data in IPC format
     * @throws IOException
     */
    private byte[] vectorsToIPC(List<FieldVector> columns) throws IOException {
        List<Field> fields = new ArrayList<Field>();
        for (FieldVector column : columns) {
            fields.add(column.getField());
        }

        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(fields, columns);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, Channels.newChannel(outputStream));
        writer.start();
        writer.writeBatch();
        writer.end();
        return outputStream.toByteArray();
    }

    /**
     * Writes task parameters (in Arrow vector format) to a FlatBuffer builder
     * @param builder FlatBuffer builder to write task parameters to
     * @param columns Task parameters encoded as a list of FieldVector
     * @return offset of the FlatBuffer builder after write operation
     * @throws IOException
     */
    private int writeTaskParameter(FlatBufferBuilder builder, List<FieldVector> columns) throws IOException {
        byte[] paramTableBytes = vectorsToIPC(columns);
        int paramsValueOffset = builder.createByteVector(paramTableBytes);
        int keyOffset = builder.createString("params");
        TaskParameter.startTaskParameter(builder);
        TaskParameter.addValue(builder, paramsValueOffset);
        TaskParameter.addKey(builder, keyOffset);
        return TaskParameter.endTaskParameter(builder);
    }

    /**
     * Return UUID object as a byte array
     * @param uuid UUID object
     * @return byte array representation of UUID object
     */
    private byte[] UUIDToBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }
}
