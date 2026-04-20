import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Broker {

    private static final int PORT = 9092;

    // Topic -> Messages

    private static final int DEFAULT_PARTITIONS = 3;
    private static final Map<String, Map<Integer, List<String>>> topics = new ConcurrentHashMap<>();

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
                            topics.putIfAbsent(topic, new ConcurrentHashMap<>());
                            Map<Integer, List<String>> partitions = topics.get(topic);
                            if (partitions.isEmpty()) {  // Initialize partitions on first use
                                for (int i = 0; i < DEFAULT_PARTITIONS; i++) {
                                    partitions.put(i, new CopyOnWriteArrayList<>());
                                }
                            }
                            int partition = Math.abs(message.hashCode()) % DEFAULT_PARTITIONS;
                            partitions.get(partition).add(message);
                            System.out.println("Stored in [" + topic + ":" + partition + "]: " + message);
                            writer.write("ACK\n");
                            writer.flush();
                        }
                        

                        // ---------------- CONSUME ----------------
                        case "CONSUME" -> {
                            if (parts.length < 4) {
                                writer.write("ERROR: Usage CONSUME <group> <topic> <partition>\n");
                                writer.flush();
                                continue;
                            }
                            String group = parts[1];
                            String topic = parts[2];
                            int partition = Integer.parseInt(parts[3]);
                            Map<Integer, List<String>> partitions = topics.get(topic);
                            if (partitions == null || !partitions.containsKey(partition)) {
                                writer.write("ERROR: Topic/partition not found\n");
                                writer.flush();
                                continue;
                            }
                            List<String> messages = partitions.get(partition);
                            offsets.putIfAbsent(group, new ConcurrentHashMap<>());
                            offsets.get(group).putIfAbsent(topic, new ConcurrentHashMap<>());
                            offsets.get(group).get(topic).putIfAbsent(partition, 0);
                            int offset = offsets.get(group).get(topic).get(partition);
                            if (offset >= messages.size()) {
                                writer.write("EMPTY\n");
                                writer.flush();
                                continue;
                            }
                            String message = messages.get(offset);
                            offsets.get(group).get(topic).put(partition, offset + 1);
                            writer.write("GROUP " + group + "\nTOPIC " + topic + "\nPARTITION " + partition + "\nMESSAGE " + message + "\nEND\n");
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