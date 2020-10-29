package com.draphony.functions;

import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasQueryParameters;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.sas.SasProtocol;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.QueueMessageItem;
import com.microsoft.azure.cognitiveservices.vision.computervision.ComputerVisionClient;
import com.microsoft.azure.cognitiveservices.vision.computervision.ComputerVisionManager;
import com.microsoft.azure.cognitiveservices.vision.computervision.models.ImageAnalysis;
import com.microsoft.azure.cognitiveservices.vision.computervision.models.VisualFeatureTypes;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Azure Functions with Timer trigger.
 */
public class ImageProcessor {
    /**
     * This function will be invoked periodically according to the specified schedule.
     */
    @FunctionName("ImageProcessor")
    public void run(
            @TimerTrigger(name = "timerInfo", schedule = "*/30 * * * * *") String timerInfo,
            final ExecutionContext context)
    {
        context.getLogger().info("Java Timer trigger function executed at: " + LocalDateTime.now());

        /**
         * In production case, it is recommended to store credentials in Azure KeyVault and configure RBAC.
         * For this lab, we just save them in sourcecode. Don't do that in production!
         */
        final String accountName = "workload20191123";
        final String accountKey = "SqzW24bR/hFfTZzP82b5YeiaE6+KvmRMFqHekEqkJE3V0WqBqHf1P/pHqS3AEZPU05oxLgGWi0lTxwNoDPFhHg==";
        final String connectionString = "DefaultEndpointsProtocol=https;AccountName=workload20191123;AccountKey=SqzW24bR/hFfTZzP82b5YeiaE6+KvmRMFqHekEqkJE3V0WqBqHf1P/pHqS3AEZPU05oxLgGWi0lTxwNoDPFhHg==;EndpointSuffix=core.windows.net";
        final String queueName = "workload";
        final String containerName = "uploads";
        final String cosmosDb = "mongodb://labs3-cosmosdb:iVdYolSLya8dr8xjc9N7P4EFjw8b1nDu7yMbkX2M4BjLiIEHkTOz84giH8zjq8bVoXbuMBARm1tdIFIjECVhpw==@labs3-cosmosdb.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@labs3-cosmosdb@";

        /**
         * Get the next message and retrieve the blob file name.
         */
        QueueClient queueClient = new QueueClientBuilder()
            .connectionString(connectionString)
            .queueName(queueName)
            .buildClient();
        final QueueMessageItem message = queueClient.receiveMessage();
        final Document messageAsJson = Document.parse(message.getMessageText());
        final String blobName = messageAsJson.get("raw").toString();

        /**
         * The blob itself is not accessible by other services.
         * So we need to generate a SAS to get the blob.
         */
        BlobServiceSasSignatureValues sasBuilder = new BlobServiceSasSignatureValues()
            .setProtocol(SasProtocol.HTTPS_ONLY)
            .setExpiryTime(OffsetDateTime.now().plusMinutes(15))
            .setContainerName(containerName)
            .setBlobName(blobName)
            .setPermissions(new BlobSasPermission().setReadPermission(true));
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        BlobServiceSasQueryParameters sasQueryParameters = sasBuilder.generateSasQueryParameters(credential);
        final String sasUri = "https://workload20191123.blob.core.windows.net/uploads/"
            + blobName
            +  "?" + sasQueryParameters.encode();

        /**
         * Use the Cognitive Services to analyse image.
         */
        final ComputerVisionClient computerVisionClient = ComputerVisionManager
            .authenticate("eabdc39e9c6144318cc95ccaa7ead4fd")
            .withEndpoint("https://westeurope.api.cognitive.microsoft.com/");
        List<VisualFeatureTypes> visualFeatureTypes = new LinkedList<VisualFeatureTypes>();
        visualFeatureTypes.add(VisualFeatureTypes.ADULT);
        visualFeatureTypes.add(VisualFeatureTypes.CATEGORIES);
        visualFeatureTypes.add(VisualFeatureTypes.COLOR);
        visualFeatureTypes.add(VisualFeatureTypes.DESCRIPTION);
        visualFeatureTypes.add(VisualFeatureTypes.FACES);
        visualFeatureTypes.add(VisualFeatureTypes.IMAGE_TYPE);
        visualFeatureTypes.add(VisualFeatureTypes.TAGS);

        final ImageAnalysis analysis = computerVisionClient.computerVision()
            .analyzeImage()
            .withUrl(sasUri)
            .withVisualFeatures(visualFeatureTypes)
            .execute();

        /**
         * Resize images
         * Option 1: @see https://docs.microsoft.com/en-us/rest/api/cognitiveservices/computervision/generatethumbnail/generatethumbnail
         *      => but it does not support images larger then 1024x1024
         * Option 2: use some other libraries
         */
        // TODO: Resize images and write resultJson into a Storage account
//        final InputStream f480 = computerVisionClient.computerVision()
//            .generateThumbnail()
//            .withWidth(1920)
//            .withHeight(1080)
//            .withUrl(sasUri)
//            .execute();
        

        /**
         * Create meta data as json and write it into CosmosDB
         */
        final Document resultJson = new Document()
            .append("version", 1.0)
            .append("date", new Date().toString())
            // .append("analysis", /* extract analysis object */)
            .append("blobName", "blobName")
            .append("blobContainer", containerName)
            .append("blobAccount", accountName)
            .append("formats", new Document()
                .append("1920x1080", "---not-done-yet---")
                .append("1280x720", "---not-done-yet---")
                .append("640x480", "---not-done-yet---")
            );

        ConnectionString connString = new ConnectionString(cosmosDb);
        final MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
            .applyConnectionString(connString)
            .retryWrites(false)
            .build();
        final MongoClient mongoClient = MongoClients.create(mongoClientSettings);
        final MongoDatabase imageDb = mongoClient.getDatabase("images");

        final MongoCollection<Document> userUploads = imageDb.getCollection("userUploads");
        userUploads.insertOne(resultJson);
        mongoClient.close();

        /** Delete message after job is completed
         * What happens if 60 seconds passed and meanwhile another azure functions got the queue message?
         *  => Your popReceipt will change!
         */
        queueClient.deleteMessage(message.getMessageId(), message.getPopReceipt());
    }
}
