package com.draphony.functions;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.azure.storage.blob.sas.*;
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
import com.mongodb.client.*;

import org.bson.Document;

/**
 * Azure Functions with Timer trigger.
 */
public class ImageProcessor {
    private static final String NOT_DONE_YET = "---not-done-yet---";
    private static final String VERSION_METADATA = "1.0";

    /**
     * This function will be invoked periodically according to the specified
     * schedule.
     */
    @FunctionName("ImageProcessor")
    public void run(@TimerTrigger(name = "timerInfo", schedule = "*/30 * * * * *") String timerInfo,
            final ExecutionContext context) {
        final Logger logger = context.getLogger();
        if (logger.isLoggable(Level.INFO))
            logger.info("Java Timer trigger function executed at: " + LocalDateTime.now());

        /**
         * In production case, it is recommended to store credentials in Azure KeyVault
         * and configure RBAC. For this lab, we just save them in sourcecode.
         * <p>
         * Don't do that in production!
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
        QueueClient queueClient = new QueueClientBuilder().connectionString(connectionString).queueName(queueName)
                .buildClient();
        final QueueMessageItem message = queueClient.receiveMessage();
        final Document messageAsJson = Document.parse(message.getMessageText());
        final String blobName = messageAsJson.get("raw").toString();

        /**
         * The blob itself is not accessible by other services. So we need to generate a
         * SAS to get the blob.
         */
        BlobServiceSasSignatureValues sasBuilder = new BlobServiceSasSignatureValues()
                .setProtocol(SasProtocol.HTTPS_ONLY).setExpiryTime(OffsetDateTime.now().plusMinutes(15))
                .setContainerName(containerName).setBlobName(blobName)
                .setPermissions(new BlobSasPermission().setReadPermission(true));
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        BlobServiceSasQueryParameters sasQueryParameters = sasBuilder.generateSasQueryParameters(credential);
        final String sasUri = "https://" + accountName + ".blob.core.windows.net/" + containerName + "/" + blobName
                + "?" + sasQueryParameters.encode();

        /**
         * Use the Cognitive Services to analyse image.
         */
        final ComputerVisionClient computerVisionClient = ComputerVisionManager
                .authenticate("eabdc39e9c6144318cc95ccaa7ead4fd")
                .withEndpoint("https://westeurope.api.cognitive.microsoft.com/");
        List<VisualFeatureTypes> visualFeatureTypes = Arrays.asList(VisualFeatureTypes.ADULT,
                VisualFeatureTypes.CATEGORIES, VisualFeatureTypes.COLOR, VisualFeatureTypes.DESCRIPTION,
                VisualFeatureTypes.FACES, VisualFeatureTypes.IMAGE_TYPE, VisualFeatureTypes.TAGS);

        final ImageAnalysis imageAnalysis = computerVisionClient.computerVision().analyzeImage().withUrl(sasUri)
                .withVisualFeatures(visualFeatureTypes).execute();

        /**
         * Resize images Option 1: @see
         * https://docs.microsoft.com/en-us/rest/api/cognitiveservices/computervision/generatethumbnail/generatethumbnail
         * => but it does not support images larger then 1024x1024 Option 2: use some
         * other libraries
         */
        // TODO: Resize images and write resultJson into a Storage account
        // final InputStream f480 = computerVisionClient.computerVision()
        // .generateThumbnail()
        // .withWidth(1920)
        // .withHeight(1080)
        // .withUrl(sasUri)
        // .execute();

        /**
         * Create meta data as json and write it into CosmosDB
         */
        final Document resultJson = new Document().append("version", VERSION_METADATA)
                .append("date", new Date().toString()).append("analysis", imageAnalysis) //
                .append("blobAccount", accountName) //
                .append("blobContainer", containerName) //
                .append("blobName", blobName) //
                .append("formats", new Document() //
                        .append("1920x1080", NOT_DONE_YET) //
                        .append("1280x720", NOT_DONE_YET) //
                        .append("640x480", NOT_DONE_YET));

        ConnectionString connString = new ConnectionString(cosmosDb);
        final MongoClientSettings mongoClientSettings = MongoClientSettings.builder().applyConnectionString(connString)
                .retryWrites(false).build();
        final MongoClient mongoClient = MongoClients.create(mongoClientSettings);
        final MongoDatabase imageDb = mongoClient.getDatabase("images");

        final MongoCollection<Document> userUploads = imageDb.getCollection("userUploads");
        userUploads.insertOne(resultJson);
        mongoClient.close();

        /**
         * Delete message after job is completed. What happens if 60 seconds passed and
         * meanwhile another azure functions got the queue message? => Your popReceipt
         * will change, QueueStorageException!
         */
        queueClient.deleteMessage(message.getMessageId(), message.getPopReceipt());
    }
}
