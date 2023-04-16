package cs642.pa2.person_recognition;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.s3.S3Client;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import software.amazon.awssdk.services.sqs.SqsClient;


/**
 * Lambda function entry point. You can change to use other pojo type or implement
 * a different RequestHandler.
 *
 * @see <a href=https://docs.aws.amazon.com/lambda/latest/dg/java-handler.html>Lambda Java Handler</a> for more information
 */
public class PersonRecognition implements RequestHandler<SQSEvent, Void> {
    private final S3Client s3Client;
    private final RekognitionClient rekognitionClient;
    private final SqsClient sqsClient;

    private final String bucketName;
    private final String car_queueURL;
    private final String people_queueURL;

    public PersonRecognition() {
        bucketName = "pa2-debug";
        car_queueURL = "https://sqs.us-west-2.amazonaws.com/608375520976/car_indexes.fifo";
        people_queueURL = "https://sqs.us-west-2.amazonaws.com/608375520976/people_indexes.fifo";

        s3Client = DependencyFactory.s3Client();
        rekognitionClient = DependencyFactory.rekognitionClient();
        sqsClient = DependencyFactory.sqsClient();
    }

    @Override
    public Void handleRequest(final SQSEvent event, final Context context) {

        for (SQSMessage msg : event.getRecords()) {
            System.out.println(new String(msg.getBody()));
        }

        return null;
    }

    /* Make function that takes in list of indexes */
    /* Make a function that takes a single image to generate image labels */
    /* Make a is_person_detected function to find a person in a image */
    /* enqueue image */
    /* enqueue stop */
}
