package helloworld;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;

import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Handler for requests to Lambda function.
 */
public class App implements RequestHandler<SQSEvent, Void> {

    private LambdaLogger logger;
    private S3Client s3Client;
    private RekognitionClient rekognitionClient;
    private SqsClient sqsClient;

    private String bucketName;
    private String car_queueURL;
    private String people_queueURL;


    private void initialize_services(Context context) {

        this.logger = context.getLogger();
        this.s3Client = S3Client.builder().build();
        this.rekognitionClient = RekognitionClient.builder().build();
        this.sqsClient = SqsClient.builder().build();

    }

    @Override
    public Void handleRequest(SQSEvent sqsEvent, Context context) {

        initialize_services(context);

        for (SQSEvent.SQSMessage message : sqsEvent.getRecords()) {
            logger.log(message.getBody());
        }

        return null;
    }

    /* Make function to check if person is detected. If so enqueue message to new queue. If message is a stop signal, break out of the while-loop */
}
