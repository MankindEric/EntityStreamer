package com.serverless;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Handler implements RequestHandler<SNSEvent, Object> {

    private static final String ENTITY_NAME_KEY = "entityName";

    private static final String TOKEN_KEY = "token";

    private static final String BUCKET_NAME = "lims-ca-notification";

    @Override
    public Object handleRequest(SNSEvent request, Context context) {
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
        context.getLogger().log("Invocation started: " + timeStamp);
        context.getLogger().log(request.getRecords().get(0).getSNS().toString());

        SNSEvent.SNS sns = request.getRecords().get(0).getSNS();
        persistData(sns);

        timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
        context.getLogger().log("Invocation completed: " + timeStamp);
        return null;
    }

    private void persistData(SNSEvent.SNS sns) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        String objectKey = String.format("%s/%s", sns.getMessageAttributes().get(ENTITY_NAME_KEY).getValue(), sns.getMessageAttributes().get(TOKEN_KEY).getValue());
        s3Client.putObject(BUCKET_NAME, objectKey, sns.getMessage());
    }
}
