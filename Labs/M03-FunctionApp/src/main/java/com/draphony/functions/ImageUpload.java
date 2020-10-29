package com.draphony.functions;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.SendMessageResult;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import org.bson.Document;

import java.io.ByteArrayInputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

/**
 * Azure Functions with HTTP Trigger.
 */
public class ImageUpload {
    /**
     * Client needs to send HTTP-POST requests with 'Content-Type' set to 'application/octet-stream'.
     * In addition a query parameter fileType is required. Supported values are 'jpeg', 'png', 'bmp'.
     */
    @FunctionName("ImageUpload")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", dataType = "binary", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<byte[]> request,
            final ExecutionContext context)
    {
        context.getLogger().info("Java HTTP trigger processed a request.");
        final String connectionString = "DefaultEndpointsProtocol=https;AccountName=workload20191123;AccountKey=SqzW24bR/hFfTZzP82b5YeiaE6+KvmRMFqHekEqkJE3V0WqBqHf1P/pHqS3AEZPU05oxLgGWi0lTxwNoDPFhHg==;EndpointSuffix=core.windows.net";

        BlobContainerClient blobContainerClient = new BlobContainerClientBuilder()
            .connectionString(connectionString)
            .containerName("uploads")
            .buildClient();

        final String fileType = request.getHeaders().get("fileType");
        context.getLogger().info("fileType" + fileType);
        final HashMap<String, String> supportFileType = new HashMap<>(2);
        supportFileType.put("image/jpeg", ".jpg");
        supportFileType.put("image/png", ".png");

        BlockBlobClient blockBlobClient = blobContainerClient
            .getBlobClient(UUID.randomUUID().toString() + supportFileType.get(fileType))
            .getBlockBlobClient();

        blockBlobClient.upload(
            new ByteArrayInputStream(request.getBody()),
            request.getBody().length
        );

        // HTTP-Request Header needs to be set as 'application/octet-stream'.
        // Otherwise the dataType of request is string not byte[]. So we need to change it when saving the blob.
        //  => Consider using client with Base64 support
        blockBlobClient.setHttpHeaders(new BlobHttpHeaders().setContentType(fileType));

        // Saving Workload to Queue as JSON
        final QueueClient queueClient = new QueueClientBuilder()
            .connectionString(connectionString)
            .queueName("workload")
            .buildClient();

        final Document msg = new Document()
            .append("version", 1.0)
            .append("date", new Date().toString())
            .append("raw", blockBlobClient.getBlobName());
        final SendMessageResult sendMessageResult = queueClient.sendMessage(msg.toJson());

        final String resultJson = msg
            .append("expirationTime",   sendMessageResult.getExpirationTime().toString())
            .append("messageId",        sendMessageResult.getMessageId())
            .append("timeNextVisible",  sendMessageResult.getTimeNextVisible().toString())
            .toJson();
        return request
            .createResponseBuilder(HttpStatus.OK)
            .body(resultJson)
            .build();
    }
}
