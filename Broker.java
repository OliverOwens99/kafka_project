import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Broker {

    private static final int PORT = 9092;

    // Topic -> Messages
    private static final Map<String, List<String>> topics = new ConcurrentHashMap<>();

    private static final Map<String, Map<String, List<String>>> consumerGroups =
        new ConcurrentHashMap<>();

    // group -> topic -> partition -> offset
    private static final Map<String, Map<String, Map<Integer, Integer>>> offsets =
        new ConcurrentHashMap<>();

    public static void main(String[] args) {
        System.out.println("Broker started on port " + PORT);

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client: " + clientSocket.getInetAddress());

                new Thread(new ClientHandler(clientSocket)).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class ClientHandler implements Runnable {
        private final Socket socket;

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {

            try (
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(socket.getOutputStream()))
            ) {

                String line;

                while ((line = reader.readLine()) != null) {
                    System.out.println("Received: " + line);

                    String[] parts = line.split(" ", 3);
                    String command = parts[0].toUpperCase();

                    switch (command) {

                        // ---------------- PRODUCE ----------------
                        case "PRODUCE" -> {
                            if (parts.length < 3) {
                                writer.write("ERROR: Usage PRODUCE <topic> <message>\n");
                                writer.flush();
                                continue;
                            }

                            String topic = parts[1];
                            String message = parts[2];

                            topics.putIfAbsent(topic, new CopyOnWriteArrayList<>());
                            topics.get(topic).add(message);

                            System.out.println("Stored in [" + topic + "]: " + message);

                            writer.write("ACK\n");
                            writer.flush();
                        }

                        // ---------------- CONSUME ----------------
                        case "CONSUME" -> {

                        if (parts.length < 3) {
                            writer.write("ERROR: Usage CONSUME <group> <topic>\n");
                            writer.flush();
                            continue;
                        }

                        String group = parts[1];
                        String topic = parts[2];

                        // Register group + topic
                        consumerGroups.putIfAbsent(group, new ConcurrentHashMap<>());
                        consumerGroups.get(group).putIfAbsent(topic, new CopyOnWriteArrayList<>());

                        List<String> messages = topics.getOrDefault(topic, new ArrayList<>());
                        List<String> groupConsumers = consumerGroups.get(group).get(topic);

                        // Register THIS connection as a consumer (simplified identity)
                        String consumerId = socket.getInetAddress().toString() + ":" + socket.getPort();

                        if (!groupConsumers.contains(consumerId)) {
                            groupConsumers.add(consumerId);
                        }

                        if (messages.isEmpty()) {
                            writer.write("EMPTY\n");
                            writer.flush();
                            continue;
                        }

                        // Round-robin assignment
                        int index = Math.abs(consumerId.hashCode()) % messages.size();

                        String assignedMessage = messages.get(index);

                        writer.write("GROUP " + group + "\n");
                        writer.write("TOPIC " + topic + "\n");
                        writer.write("MESSAGE " + assignedMessage + "\n");
                        writer.write("END\n");
                        writer.flush();
                    }

                        default -> {
                            writer.write("ERROR: Unknown command\n");
                            writer.flush();
                        }
                    }
                }

            } catch (IOException e) {
                System.out.println("Client disconnected.");
            }
        }
        


    }
}