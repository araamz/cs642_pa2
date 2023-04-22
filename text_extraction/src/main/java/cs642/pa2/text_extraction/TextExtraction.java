package cs642.pa2.text_extraction;

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
        /*

            Setting Application Variables: necessary parameters to do two of the following operations.

                1. bucketName: this variable is used to set the bucket for which images are initially pulled from for
                processing.

                2. queueURL: this variable is used to set the queue to read the processed images with people above the
                90% threshold.

                3. outputFile: this variable determines for where to write the found text for each image.

        */
        bucketName = "cs442-unr";
        queueURL = "https://sqs.us-west-2.amazonaws.com/608375520976/people_indexes.fifo";
        outputFile = "output.txt";

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

        // Note: the function creates a file to store the text detected or if the file exists, clears the output file.
        initialize_file();

        while (true) {

            // Note: For each iteration, the image name is saved for later use in the iteration by dequeing a message
            // from the people indexes queue.
            String image = dequeue_message();

            // Note: if the "-1" stop signal is detected, the function breaks the while-loop to stop execution of the
            // application as the last message has arrived.
            if (image.equals("-1")) {
                System.out.println("Stop Detected - shutting down now");
                break;
            }

            // Note: the detected text is processed as one large concatenated string and then used with the helper function
            // append_file() to append to the output file with the image name and text itself.
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

    /*

        generate_image_text: this function is responsible for grabbing the text within a image and storing each string
        of text identified in a list.

        Summary: The function takes in a image, then grabs the image as a S3Object using the passed in image name.
        From this, a Image object is created in which the returned S3Object is used to create the image object.
        This image object is then used to create a request object for text detection in which is executed by the detect
        text function. This action results in a list of identified strings of text within the image. If not strings of
        text are detected, the function returns null.

    */
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

    /*

        read_detected_text: this function is responsible for reading the strings detected from a image and then creating
        a single concatenated string from all the identified text strings.

        Summary: The function takes in a image, then grabs the image as a S3Object using the passed in image name.
        From this, a helper function generate_image_text() is used to create a list of detected text objects which then
        are further processed by the function. If no strings are detected, then the function returns a empty string.
        Otherwise, a empty string is created called concatenated_text in which the list storing each detected string
        (image_text) is iterated upon. For each iterated piece of text, the text is appended to the concatenated_text
        each iteration. Once fully iterated, the concatenated string is returned.

    */
    private String read_detected_text(String image_name) {
        List<TextDetection> image_text = generate_image_text(image_name);

        if (image_text == null) {
            return "";
        }

        String concatenated_text = "";
        for (TextDetection text : image_text) {
            concatenated_text = concatenated_text + " " + text.detectedText();
        }

        System.out.println("Detected Text - " + image_name + ": " + concatenated_text);
        return concatenated_text;

    }

    /*

        dequeue_message: this function is responsible for dequeing an image name from the people indexes queue. The
        function returns the string that is stored in the message body.

        Summary: The function creates a request object to grab a message from the people indexes queue. The function
        long polls the people indexes queue. Once the message response is returned, the message body of the response
        is then saved to be returned. Before returning the string, the function deletes the message from the people
        indexes queue using delete_message() to single process a message.

    */
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

    /*

        initialize_file: this function is used  for creating the output file to store the identified images results
        at the end of the data pipeline.

        Summary: The function first checks if the output file already exists, in the event the file does exist it will
        be deleted. This is to remove previous results of the data pipeline. If the file does not exist, then the
        output file is created for use of appending results to the file. 

    */
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
