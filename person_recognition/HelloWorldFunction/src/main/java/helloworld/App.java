package helloworld;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.List;

/**
 * Handler for requests to Lambda function.
 */
public class App implements RequestHandler<SQSEvent, Void> {
    private LambdaLogger logger;
    private RekognitionClient rekognitionClient;
    private SqsClient sqsClient;
    private String bucketName;
    private String queueURL;

    @Override
    public Void handleRequest(SQSEvent event, Context context) {

        initialize_services(context);
        logger.log("Car Record Count: " + event.getRecords().size());
        for (SQSEvent.SQSMessage message : event.getRecords()) {

            String image = message.getBody();

            if (image.equals("-1")) {
                System.out.println("Stop Detected - shutting down now");
                break;
            }

            logger.log("Processing Image: " + image);
            if (is_person_detected(image)) {
                enqueue_image(image);
            }

        }

        enqueue_stop();
        this.rekognitionClient.close();
        this.sqsClient.close();

        return null;
    }

    private void initialize_services(Context context) {

        this.logger = context.getLogger();
        this.rekognitionClient = RekognitionClient.builder().region(Region.US_EAST_1).build();
        this.sqsClient = SqsClient.builder().build();

        this.bucketName = "cs442-unr";
        this.queueURL = "https://sqs.us-west-2.amazonaws.com/608375520976/people_indexes.fifo";

    }

    private List<Label> generate_image_labels(String image_name) {

        try {
            System.out.println("Getting labels: " + image_name);
            S3Object retrieved_image = S3Object.builder().bucket(bucketName).name(image_name).build();

            Image unlabeled_image = Image.builder().s3Object(retrieved_image).build();

            DetectLabelsRequest labels_request = DetectLabelsRequest.builder().image(unlabeled_image).build();

            DetectLabelsResponse labels_response = rekognitionClient.detectLabels(labels_request);

            return labels_response.labels();

        } catch (RekognitionException e) {
            logger.log(String.valueOf(e));
        }

        return null;

    }

    private boolean is_person_detected(String image_name) {
        List<Label> image_labels = generate_image_labels(image_name);

        if (image_labels == null) {
            return false;
        }

        for (Label label : image_labels) {
            if (label.name().equals("Person") && (label.confidence() > 90.0F)) {
                logger.log("Person Detected: " + image_name + " Confidence: " + label.confidence());
                return true;
            } else if (label.name().equals("Person") && (label.confidence() <= 90.0F)) {
                logger.log("Person Not Detected: " + image_name + " Confidence: " + label.confidence());
            }
        }

        logger.log("Person Not Detected: " + image_name);

        return false;
    }

    private void enqueue_image(String image_name) {

        String deduplication_id = "people_indexes:" + image_name;
        String message_group_id = "people_indexes";
        SendMessageRequest image_request = SendMessageRequest.builder().queueUrl(queueURL).messageBody(image_name).messageDeduplicationId(deduplication_id).messageGroupId(message_group_id).build();

        sqsClient.sendMessage(image_request);
        System.out.println("Enqueuing Image: " + image_name);
    }

    private void enqueue_stop() {

        String deduplication_id = "people_indexes:" + "-1";
        String message_group_id = "people_indexes";
        SendMessageRequest stop_request = SendMessageRequest.builder().queueUrl(queueURL).messageBody("-1").messageDeduplicationId(deduplication_id).messageGroupId(message_group_id).build();

        sqsClient.sendMessage(stop_request);
        System.out.println("Enqueuing Stop Message");

    }

}
