package ca.concordia.server;
import ca.concordia.filesystem.FileSystemManager;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class FileServer {
    private final FileSystemManager fsManager;
    private final int port;

    public FileServer(int port, String fileSystemName, int totalSize) {
        this.port = port;
        try {
            this.fsManager = new FileSystemManager(fileSystemName, totalSize);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize filesystem", e);
        }
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started. Listening on port " + port + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Handling client: " + clientSocket);
                new Thread(() -> handleClient(clientSocket)).start();//one client handled on one thread 
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not start server on port " + port);
        }
    }

    private void handleClient(Socket clientSocket) { //Handles single client 
        try (
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            String line = reader.readLine();
            if (line == null || line.trim().isEmpty()) {
                writer.println("ERROR: empty command");
                return;
            }
            String[] parts = line.trim().split(" ", 3);
            String command = parts[0].toUpperCase();
            try {
                switch (command) {
                    case "CREATE" -> {
                        if (parts.length < 2) {
                            writer.println("ERROR: missing filename");
                        } else {
                            fsManager.createFile(parts[1]);
                            writer.println("SUCCESS: File '" + parts[1] + "' created.");
                        }
                    }
                    case "WRITE" -> {
                        if (parts.length < 3) {
                            writer.println("ERROR: missing content");
                        } else {
                            byte[] data = parts[2].getBytes(StandardCharsets.UTF_8);
                            fsManager.writeFile(parts[1], data);
                            writer.println("SUCCESS: wrote " + data.length + " bytes to '" + parts[1] + "'");
                        }
                    }
                    case "READ" -> {
                        if (parts.length < 2) {
                            writer.println("ERROR: missing filename");
                        } else {
                            byte[] data = fsManager.readFile(parts[1]);
                            String content = new String(data, StandardCharsets.UTF_8);
                            writer.println("SUCCESS: " + content);
                        }
                    }
                    case "DELETE" -> {
                        if (parts.length < 2) {
                            writer.println("ERROR: missing filename");
                        } else {
                            fsManager.deleteFile(parts[1]);
                            writer.println("SUCCESS: File '" + parts[1] + "' deleted.");
                        }
                    }
                    case "LIST" -> {
                        String[] files = fsManager.listFiles();
                        writer.println("SUCCESS: " + String.join(" ", files));
                    }
                    case "QUIT" -> writer.println("SUCCESS: Disconnecting.");
                    default -> writer.println("ERROR: Unknown command.");
                }
            } catch (Exception ex) {
                writer.println("ERROR: " + ex.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (Exception ignored) {
            }
        }
    }
}

