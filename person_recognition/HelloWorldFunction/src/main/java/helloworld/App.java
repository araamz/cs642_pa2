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

/*

    App: The class runs a handle request handler that is used to process incoming SQS messages from the car indexes
    queue. The handler readies the necessary services to process incoming image names, process images, and then finally
    execute code to add it to the people queue.

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
        // Note: Iterate over all the records from a given SQS event and its attached records. Each message would be
        // processed for each iteration.
        for (SQSEvent.SQSMessage message : event.getRecords()) {

            // Note: get the name of the image by accessing the message body.
            String image = message.getBody();

            // Note: if the stop signal is detected in the records, suspend processing as it means there will be no
            // more items to process. Thus, pass along the stop signal by enqueuing the stop signal to the people indexes
            // queue and then break the for-loop.
            if (image.equals("-1")) {
                System.out.println("Stop Detected - shutting down now");
                enqueue_stop();
                break;
            }

            logger.log("Processing Image: " + image);
            // Note: If a person is detected within a image, then the name of the current processing image name is
            // enqueued to the people indexes queue.
            if (is_person_detected(image)) {
                enqueue_image(image);
            }

        }

        this.rekognitionClient.close();
        this.sqsClient.close();

        return null;
    }

    /*

        initialize_services: this function is used to initialize the necessary clients used to access AWS services.

     */
    private void initialize_services(Context context) {

        /*

            Setting AWS Services Instances: necessary initialization of services needed to connect to the following AWS
            services.

                1. logger: this service is used to access CloudWatch for monitoring of the Lambda function execution.

                2. rekognitionClient: this service is used to create the labels as part of processing. The labels to be
                identified by the client are Cars found in the image with 90% or more confidence.

                3. sqsClient: this service is used to communicate to the people indexes queue when a image has been
                processed with the required parameters.

        */
        this.logger = context.getLogger();
        this.rekognitionClient = RekognitionClient.builder().region(Region.US_EAST_1).build();
        this.sqsClient = SqsClient.builder().build();

        /*

            Setting Application Variables: necessary parameters to do two of the following operations.

                1. bucketName: this variable is used to set the bucket for which images are pulled from for
                additional processing.

                2. queueURL: this variable is used to set the queue to send the processed images with people above the
                90% threshold.

        */
        this.bucketName = "cs442-unr";
        this.queueURL = "https://sqs.us-west-2.amazonaws.com/608375520976/people_indexes.fifo";

    }

    /*

        generate_image_labels: this function is responsible for grabbing the labels from a image given a image name.

        Summary: The function takes in a image, then grabs the image as a S3Object using the passed in image name.
        From this, a Image object is created in which the returned S3Object is used to create the image object.
        This image object is then used to create a request object for labels in which is executed by the detect labels
        function of Rekognition. This action results in a list of Label objects which are then returned. If no labels
        are detected, then the function returns null.

    */
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

    /*

            is_person_detected: this function is responsible for ensuring the required confidence levels are met for image
            processing.

            Summary: The function takes in a image name, in which labels are detected using the helper function
            generate_image_labels(). The labels of the image are saved in a list of Label objects in which each label
            object is iterated. During each iteration, the image label is checked for the label for "People"
            and label confidence is greater than 90%. If that requirement is met, then the function stops and returns a
            boolean to state a image is found with a car with high confidence. Otherwise, the function returns false.

    */
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

    /*

        enqueue_image: this function is used to enqueue image names to the people indexes queue. Images are added into a FIFO
        list within a message group called people_indexes.

        Summary: The function prepares several variables used to define the deduplication_id and message_group_id used
        to operate the queue. The request to send the passed in image name is built using the previously created
        variables. Once created, the message is sent to the SQS queue for people indexes.

    */
    private void enqueue_image(String image_name) {

        String deduplication_id = "people_indexes:" + image_name;
        String message_group_id = "people_indexes";
        SendMessageRequest image_request = SendMessageRequest.builder().queueUrl(queueURL).messageBody(image_name).messageDeduplicationId(deduplication_id).messageGroupId(message_group_id).build();

        sqsClient.sendMessage(image_request);
        System.out.println("Enqueuing Image: " + image_name);
    }

    /*

        enqueue_stop: this function is used to enqueue the stop signal which reports that all images have been processed
        and sent to the queue.

        Summary: The function prepares several variables used to define the deduplication_id and message_group_id used
        to operate the queue. The request then attaches a "-1" to the deduplication_id to ensure the message does not
        get lost and is sent last. The request to send the stop signal is built using the previously created
        variables. Once created, the message is sent to the SQS queue for people indexes.

    */
    private void enqueue_stop() {

        String deduplication_id = "people_indexes:" + "-1";
        String message_group_id = "people_indexes";
        SendMessageRequest stop_request = SendMessageRequest.builder().queueUrl(queueURL).messageBody("-1").messageDeduplicationId(deduplication_id).messageGroupId(message_group_id).build();

        sqsClient.sendMessage(stop_request);
        System.out.println("Enqueuing Stop Message");

    }

}
