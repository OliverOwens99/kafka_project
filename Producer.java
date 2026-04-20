import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Producer {

    private static final String HOST = "localhost";
    private static final int PORT = 9092;

    public static void main(String[] args) {
        System.out.println("Producer connected to broker at " + HOST + ":" + PORT);

        try (
            Socket socket = new Socket(HOST, PORT);
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(socket.getOutputStream()));
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));
            Scanner scanner = new Scanner(System.in)
        ) {

            while (true) {
                System.out.print("Enter topic: ");
                String topic = scanner.nextLine();

                System.out.print("Enter message: ");
                String message = scanner.nextLine();

                String payload = topic + ":" + message;

                // Send message
                writer.write(payload);
                writer.newLine();
                writer.flush();

                // Wait for ACK
                String response = reader.readLine();
                System.out.println("Broker response: " + response);
            }

        } catch (IOException e) {
            System.out.println("Connection error: " + e.getMessage());
        }
    }
}