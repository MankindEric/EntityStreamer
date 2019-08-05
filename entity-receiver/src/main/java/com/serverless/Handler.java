package com.serverless;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Handler implements RequestHandler<S3Event, Object> {

    private static final String RECEIVER_URL = "";

    private static final String DEFAULT_USER = "admin";

    private static final String DEFAULT_PASS = "abc123";

    @Override
    public Object handleRequest(S3Event s3Event, Context context) {
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
        context.getLogger().log("Invocation started: " + timeStamp);

        S3EventNotification.S3EventNotificationRecord record = s3Event.getRecords().get(0);
        String entityId = record.getS3().getObject().getKey().split("/")[1];
        context.getLogger().log("Get key: " + entityId);
        try {
            this.invokeReceiver(entityId);
        } catch (IOException e) {
            e.printStackTrace();
        }

        timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
        context.getLogger().log("Invocation completeda: " + timeStamp);
        return null;
    }

    public void invokeReceiver(String entityId) throws IOException {
        String requestUrl = String.format(RECEIVER_URL, entityId);
        HttpPost request = new HttpPost(requestUrl);
        String auth = DEFAULT_USER + ":" + DEFAULT_PASS;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);
        request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);

        HttpClient client = HttpClientBuilder.create().build();
        System.out.println("Request: " + request.toString());

        HttpResponse response = client.execute(request);

        System.out.println("Get response code: " + response.getStatusLine().getStatusCode());
        System.out.println("Get response content: " + response.getEntity().getContent().toString());
    }
}
