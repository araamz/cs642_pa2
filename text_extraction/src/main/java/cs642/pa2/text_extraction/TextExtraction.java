package cs642_pa1.text_extraction;

import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.*;
import java.util.List;

public class TextExtraction {
    private final S3Client s3Client;
    private final RekognitionClient rekognitionClient;
    private final SqsClient sqsClient;

    private final String bucketName;
    private final String queueURL;
    private final String outputFile;

    public TextExtraction() {
        bucketName = "cs442unr";
        queueURL = "https://sqs.us-west-2.amazonaws.com/608375520976/index_pipeline.fifo";
        outputFile = "output.txt";

        s3Client = DependencyFactory.s3Client();
        rekognitionClient = DependencyFactory.rekognitionClient();
        sqsClient = DependencyFactory.sqsClient();

        initialize_file();

        while (true) {

            String image = dequeue_message();

            if (image.equals("-1")) {
                System.out.println("Stop Detected - shutting down now");
                break;
            }

            String detected_text = read_detected_text(image);
            append_file(image, detected_text);

        }

        s3Client.close();
        rekognitionClient.close();
        sqsClient.close();

    }

    public static void main(String[] args) {
        new TextExtraction();
    }

    private List<TextDetection> generate_image_text(String image_name) {

        try {
            System.out.println("Extracting text: " + image_name);
            S3Object retrieved_image = S3Object.builder().bucket(bucketName).name(image_name).build();

            Image unextracted_image = Image.builder().s3Object(retrieved_image).build();

            DetectTextRequest text_request = DetectTextRequest.builder().image(unextracted_image).build();

            DetectTextResponse text_response = rekognitionClient.detectText(text_request);

            return text_response.textDetections();

        } catch (RekognitionException e) {
            System.err.println(e);
            System.exit(1);
        }

        return null;
    }

    private String read_detected_text(String image_name) {
        List<TextDetection> image_text = generate_image_text(image_name);

        if (image_text == null) {
            return "";
        }

        String concatenated_text = "";
        for (TextDetection text : image_text) {
            concatenated_text = concatenated_text + " " + text.detectedText();
        }

        return concatenated_text;

    }

    private String dequeue_message() {
        ReceiveMessageRequest message_request = ReceiveMessageRequest.builder().queueUrl(queueURL).waitTimeSeconds(20).build();

        Message message_object = sqsClient.receiveMessage(message_request).messages().get(0);
        String message = message_object.body();
        System.out.println("Dequeuing: " + message);

        delete_message(message_object);
        return message;

    }

    private void delete_message(Message message) {
        DeleteMessageRequest delete_request = DeleteMessageRequest.builder().queueUrl(queueURL).receiptHandle(message.receiptHandle()).build();

        sqsClient.deleteMessage(delete_request);
    }

    private void initialize_file() {
        File file = new File(outputFile);

        if (file.exists()) {
            System.out.println("Deleting Output File: " + outputFile);
            file.delete();
        }

        try {
            System.out.println("Creating Output File: " + outputFile);
            file.createNewFile();
        } catch (IOException e) {
            System.err.println(e);
            System.exit(1);
        }

    }

    private void append_file(String index_image, String detected_text) {

        String index_entry = index_image + "\t" + detected_text;

        FileWriter file_writer = null;
        BufferedWriter buffered_writer = null;
        PrintWriter print_writer = null;

        try {

            file_writer = new FileWriter(outputFile, true);
            buffered_writer = new BufferedWriter(file_writer);
            print_writer = new PrintWriter(buffered_writer);

            System.out.println("Adding Entry to Output File: " + index_entry);
            print_writer.println(index_entry);

            print_writer.flush();
            file_writer.close();
            buffered_writer.close();
            print_writer.close();

        } catch (IOException e) {

            System.err.println(e);
            System.exit(1);

        }

    }

}
