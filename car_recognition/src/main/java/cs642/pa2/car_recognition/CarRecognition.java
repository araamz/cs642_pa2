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

    // AWS Services Instances
    private final S3Client s3Client;
    private final RekognitionClient rekognitionClient;
    private final SqsClient sqsClient;

    // Application Variables
    private final String bucketName;
    private final String queueURL;

    public CarRecognition() {
        bucketName = "cs442-unr";
        queueURL = "https://sqs.us-west-2.amazonaws.com/608375520976/car_indexes.fifo";

        s3Client = DependencyFactory.s3Client();
        rekognitionClient = DependencyFactory.rekognitionClient();
        sqsClient = DependencyFactory.sqsClient();

        List<String> image_names = download_bucket_image_names();

        for (String image : image_names) {

            if (is_car_detected(image)) {
                enqueue_image(image);
            }

        }

        enqueue_stop();

        rekognitionClient.close();
        s3Client.close();
        sqsClient.close();

    }

    public static void main(String[] args) {
        new CarRecognition();
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
            System.err.println(e);
        }

        return null;

    }

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

    private void enqueue_image(String image_name) {

        String deduplication_id = "car_indexes:" + image_name;
        String message_group_id = "car_indexes";
        SendMessageRequest image_request = SendMessageRequest.builder().queueUrl(queueURL).messageBody(image_name).messageDeduplicationId(deduplication_id).messageGroupId(message_group_id).build();

        sqsClient.sendMessage(image_request);
        System.out.println("Enqueuing Image: " + image_name);
    }

    private void enqueue_stop() {

        String deduplication_id = "car_indexes:" + "-1";
        String message_group_id = "car_indexes";
        SendMessageRequest stop_request = SendMessageRequest.builder().queueUrl(queueURL).messageBody("-1").messageDeduplicationId(deduplication_id).messageGroupId(message_group_id).build();

        sqsClient.sendMessage(stop_request);
        System.out.println("Enqueuing Stop Message");

    }

}