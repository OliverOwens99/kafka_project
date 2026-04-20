package consumer;
import java.io.*;
import java.net.*;
import java.util.Scanner;

public class consumer {

    private static final String HOST = "localhost";
    private static final int PORT = 9092;

    public static void main(String[] args) {
        System.out.println("Consumer connected to broker at " + HOST + ":" + PORT);

        try (
            Socket socket = new Socket(HOST, PORT);
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));
            Scanner scanner = new Scanner(System.in)
        ) {

            while (true) {
                System.out.print("Enter topic to consume: ");
                String topic = scanner.nextLine();

                // Send consume request
                writer.write("CONSUME " + topic);
                writer.newLine();
                writer.flush();

                System.out.println("Messages from topic [" + topic + "]:");

                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.equals("END")) break;
                    System.out.println("> " + line);
                }
            }

        } catch (IOException e) {
            System.out.println("Connection error: " + e.getMessage());
        }
    }
}