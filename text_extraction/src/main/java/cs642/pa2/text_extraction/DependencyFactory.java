
package cs642.pa2.text_extraction;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * The module containing all dependencies required by the {@link cs642_pa1.text_extraction.TextExtraction}.
 */
public class DependencyFactory {

    private DependencyFactory() {}

    public static S3Client s3Client() {
        return S3Client.builder()
                .region(Region.US_EAST_1)
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
    }

    public static RekognitionClient rekognitionClient() {
        return RekognitionClient.builder()
                .region(Region.US_EAST_1)
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
    }

    public static SqsClient sqsClient() {
        return SqsClient.builder()
                .region(Region.US_WEST_2)
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
    }


}
