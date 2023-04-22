package cs642.pa2.car_recognition;

import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.ArrayList;
import java.util.List;

public class CarRecognition {

    // AWS Services Instances: these are instances used by the application to store connections to different services.
    private final S3Client s3Client;
    private final RekognitionClient rekognitionClient;
    private final SqsClient sqsClient;

    // Application Variables: these are variables used by the application to access the names of defined resources.
    private final String bucketName;
    private final String queueURL;

    public CarRecognition() {

        /*
            CarRecognition: The constructor runs the under laying code to process images, add images to a SQS Queue, and
            finally send the stop message to signify all photos have been sent for further processing.
        */

        /*

            Setting Application Variables: necessary parameters to do two of the following operations.

                1. bucketName: this variable is used to set the bucket for which images are initially pulled from for
                processing.

                2. queueURL: this variable is used to set the queue to send the processed images with cars above the
                90% threshold.

        */
        bucketName = "cs442-unr";
        queueURL = "https://sqs.us-west-2.amazonaws.com/608375520976/car_indexes.fifo";

        /*

            Setting AWS Services Instances: necessary initialization of services needed to connect to the following AWS
            services.

                1. s3Client: this service is used to access the global bucket that includes the images to be processed
                by the pipeline.

                2. rekognitionClient: this service is used to create the labels as part of processing. The labels to be
                identified by the client are Cars found in the image with 90% or more confidence.

                3. sqsClient: this service is used to communicate to the car indexes queue when a image has been
                processed with the required parameters.

        */
        s3Client = DependencyFactory.s3Client();
        rekognitionClient = DependencyFactory.rekognitionClient();
        sqsClient = DependencyFactory.sqsClient();

        // Note: this code downloads the image names that are within the set bucket and then stores each entry in a
        // list.
        List<String> image_names = download_bucket_image_names();

        // Note: this loop iterates through each image in the image_names list and then processes for cars.
        for (String image : image_names) {

            // Note: when iterating through each image, the image is checked for a car detected, when detected its added to
            // the car index queue.
            if (is_car_detected(image)) {
                enqueue_image(image);
            }

        }

        // Note: this code runs after each image identified from the bucket has been iterated in which the stop signal
        // is the added to the queue after processing.
        enqueue_stop();

        rekognitionClient.close();
        s3Client.close();
        sqsClient.close();

    }

    public static void main(String[] args) {
        new CarRecognition();
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
            System.err.println(e);
        }

        return null;

    }

    /*

        is_car_detected: this function is responsible for ensuring the required confidence levels are met for image
        processing.

        Summary: The function takes in a image name, in which labels are detected using the helper function
        generate_image_labels(). The labels of the image are saved in a list of Label objects in which each label
        object is iterated. During each iteration, the image label is checked for the label for "Car"
        and label confidence is greater than 90%. If that requirement is met, then the function stops and returns a
        boolean to state a image is found with a car with high confidence. Otherwise, the function returns false.

    */
    private boolean is_car_detected(String image_name) {
        List<Label> image_labels = generate_image_labels(image_name);

        if (image_labels == null) {
            return false;
        }

        for (Label label : image_labels) {
            if (label.name().equals("Car") && (label.confidence() > 90.0F)) {
                System.out.println("Car Detected: " + image_name + " Confidence: " + label.confidence());
                return true;
            } else if (label.name().equals("Car") && (label.confidence() <= 90.0F)) {
                System.out.println("Car Not Detected: " + image_name + " Confidence: " + label.confidence());
            }
        }

        System.out.println("Car Not Detected: " + image_name);

        return false;
    }

    /*

        download_bucket_image_names: this function downloads the name of files within the initialized S3 bucket.

        Summary: The function prepares a list in which is used to store strings. After preparing the list, a request object
        is created to request the list of objects in the bucket. This request would be used to develop the response object
        in which the response contains a list of the S3 contents. This would be stored in a variable called images. The image
        variable gets iterated with each iteration grabbing the key value of the object. The key value would be the name of
        S3 Object in which is then added to the image list prepared earlier. Once iteration is completed without issues, it
        is returned.

    */
    private List<String> download_bucket_image_names() {

        List<String> image_list = new ArrayList<String>();

        try {
            ListObjectsRequest bucket_objects_request = ListObjectsRequest.builder().bucket(bucketName).build();
            ListObjectsResponse bucket_objects_response = s3Client.listObjects(bucket_objects_request);
            List<software.amazon.awssdk.services.s3.model.S3Object> images = bucket_objects_response.contents();


            for (software.amazon.awssdk.services.s3.model.S3Object image_object : images) {
                image_list.add(image_object.key());
            }

        } catch (S3Exception e) {
            System.err.println(e);
            System.exit(1);
        }

        return image_list;

    }

    /*

        enqueue_image: this function is used to enqueue image names to the car indexes queue. Images are added into a FIFO
        list within a message group called car_indexes.

        Summary: The function prepares several variables used to define the deduplication_id and message_group_id used
        to operate the queue. The request to send the passed in image name is built using the previously created
        variables. Once created, the message is sent to the SQS queue for car indexes.

    */
    private void enqueue_image(String image_name) {

        String deduplication_id = "car_indexes:" + image_name;
        String message_group_id = "car_indexes";
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
        variables. Once created, the message is sent to the SQS queue for car indexes.

    */
    private void enqueue_stop() {

        String deduplication_id = "car_indexes:" + "-1";
        String message_group_id = "car_indexes";
        SendMessageRequest stop_request = SendMessageRequest.builder().queueUrl(queueURL).messageBody("-1").messageDeduplicationId(deduplication_id).messageGroupId(message_group_id).build();

        sqsClient.sendMessage(stop_request);
        System.out.println("Enqueuing Stop Message");

    }

}