import com.azure.core.http.*;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.implementation.DateTimeRfc1123;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;

public class App {
    static final String workspaceId = "9aa9f2d3-dd4f-42b3-a60d-a49aa6c19ca1";

    /**
     * Get Shared Key
     *
     * @see <a href="https://docs.microsoft.com/en-us/rest/api/loganalytics/workspaces/getsharedkeys">docs.microsoft.com/en-us/rest/api/loganalytics/workspaces/getsharedkeys</a>
     */
    static final String sharedKey = "hqd9BmXhVGNSDFgYHUhovjjj/koIDwXE7u7dVsl0Iy2h66R0QOO2taJ4nqGh1ZRFoB8fkRHw2iOwxWY1ls4UPQ==";

    public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeyException {
        final Logger logger = LoggerFactory.getLogger(App.class);

        // Add JSON into Log Analytics Workspace
        // @see <a href="https://docs.microsoft.com/en-us/rest/api/loganalytics/create-request">create-request</a>
        final String postDataUrl = String.format(
                "https://%s.ods.opinsights.azure.com/api/logs?api-version=2016-04-01", workspaceId);

        HttpClient client = new NettyAsyncHttpClientBuilder().build();

        String json = "[{\"DemoField1\":\"DemoValue1\",\"DemoField2\":\"DemoValue2\"},{\"DemoField3\":\"DemoValue3\",\"DemoField4\":\"DemoValue4\"}]";
        String contentType = "application/json";
        String logName = "TestLogType";

        DateTimeRfc1123 rfc1123 = new DateTimeRfc1123(OffsetDateTime.now());
        String dateTime = rfc1123.toString();

        byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
        String stringToHash = "POST" + "\n" //
                + jsonBytes.length + "\n" //
                + contentType + "\n" //
                + "x-ms-date:" + dateTime + "\n" //
                + "/api/logs";
        String authorization = buildAuth(workspaceId, sharedKey, stringToHash);

        HttpHeaders headers = new HttpHeaders();
        headers.put("Authorization", authorization);
        headers.put("Content-Type", contentType);
        // headers.put("Accept", contentType);
        headers.put("Log-Type", logName);
        // The date that the request was processed in RFC 1123 format
        headers.put("x-ms-date", dateTime);
        // You can use an optional field to specify the timestamp from the data.
        // If the time field is not specified, Azure Monitor assumes the time is the
        // message ingestion time
        headers.put("time-generated-field", "");

        HttpRequest dataRequest = new HttpRequest(HttpMethod.POST, postDataUrl);
        dataRequest.setHeaders(headers); // alternatively use dataRequest.setHeader()
        dataRequest.setBody(json);

        // It may takes a couple of minutes until it is visible
        Mono<HttpResponse> response = client.send(dataRequest);
        HttpResponse finalResponse = response.block();
        if (finalResponse == null)
            logger.warn("The final response is null");
        else if (logger.isInfoEnabled()) // or use this information otherwise:
            logger.info("{}: {}", finalResponse.getStatusCode(), finalResponse.getBodyAsString().block());
    }

    private static String BuildAuth(String workspaceId, String sharedKey, String stringToHash) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac mac = Mac.getInstance("HmacSHA256");

        SecretKeySpec secret_key = new SecretKeySpec(DatatypeConverter.parseBase64Binary(sharedKey), "HmacSHA256");
        mac.init(secret_key);
        String hashedString = DatatypeConverter.printBase64Binary(mac.doFinal(stringToHash.getBytes(StandardCharsets.UTF_8)));

        // return String.format("SharedKey %s:%s", workspaceId, hashedString);
        return "SharedKey " + workspaceId + ":" + hashedString;
    }
}
