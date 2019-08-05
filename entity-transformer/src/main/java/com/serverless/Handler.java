package com.serverless;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Handler implements RequestHandler<Map<String, Object>, Object> {

    private static final String BUCKET_NAME = "lims-ca-notification";

    private static final String TRIM_ENTITY_PREFIX = "lims-case-trim";

    private static final String ENTITY_PREFIX = "lims-case";

    private static final String IN_PROCESS_PREFIX = "processing";

    private static final String ARCHIVE_PREFIX = "archive";

    private static final String ENTITY_KEY = "caseId";

    private AmazonS3 s3Client;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Object handleRequest(Map<String, Object> request, Context context) {
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
        context.getLogger().log("Invocation started: " + timeStamp);
        s3Client = AmazonS3ClientBuilder.standard().build();

        String processId = UUID.randomUUID().toString();
        List<String> processObjectList = moveObject(processId);
        try {
            this.transformObject(processObjectList);
        } catch (IOException e) {
            e.printStackTrace();
            context.getLogger().log("Transform Object Error with process id: " + processId);
        }
        timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
        context.getLogger().log("Invocation completed: " + timeStamp);
        return null;
    }

    private List<String> moveObject(String processId) {
        List<S3ObjectSummary> s3ObjectSummaries = s3Client.listObjects(BUCKET_NAME, String.format("%s/", ENTITY_PREFIX)).getObjectSummaries();
        List<String> processObjectList = new ArrayList<>();

        s3ObjectSummaries.forEach(s -> {
            String originalKey = s.getKey();
            String newKey = String.format("%s/%s", IN_PROCESS_PREFIX, originalKey.replace(ENTITY_PREFIX, processId));
            s3Client.copyObject(BUCKET_NAME, originalKey, BUCKET_NAME, newKey);
            s3Client.deleteObject(BUCKET_NAME, originalKey);
            processObjectList.add(newKey);
        });

        return processObjectList;
    }

    public void transformObject(List<String> processObjectList) throws IOException {
        // Iterate process objects and put into entity map
        Map<String, String> entityMap = new HashMap<>();
        for (String processObjectKey :
                processObjectList) {
            S3Object processObject = s3Client.getObject(BUCKET_NAME, processObjectKey);
            S3ObjectInputStream contentInputStream = processObject.getObjectContent();

            String messageContent = IOUtils.toString(contentInputStream);
            JsonNode messageContentJson = mapper.readTree(messageContent);
            entityMap.put(messageContentJson.get(ENTITY_KEY).toString(), messageContent);
        }

        // Write to trim entity to S3 bucket
        entityMap.forEach((entityId, content) -> {
            String trimEntityKey = String.format("%s/%s", TRIM_ENTITY_PREFIX, entityId);
            s3Client.putObject(BUCKET_NAME, trimEntityKey, content);
        });

        // Move processed object to archive key folder
        processObjectList.forEach(processObjectKey -> {
            String archiveKey = processObjectKey.replace(IN_PROCESS_PREFIX, ARCHIVE_PREFIX);
            s3Client.copyObject(BUCKET_NAME, processObjectKey, BUCKET_NAME, archiveKey);
            s3Client.deleteObject(BUCKET_NAME, processObjectKey);
        });
    }
}
