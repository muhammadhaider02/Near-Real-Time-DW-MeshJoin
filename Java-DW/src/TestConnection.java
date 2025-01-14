import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;

public class TestConnection {

    private Connection conn;

    public TestConnection() { // Constructor
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.print("Enter MySQL database URL: jdbc:mysql://localhost:3306/Metro_DW ");
            String dbUrl = scanner.nextLine();
            System.out.print("Enter username: ");
            String user = scanner.nextLine();
            System.out.print("Enter password: ");
            String password = scanner.nextLine();

            conn = DriverManager.getConnection(dbUrl, user, password); // Establish Connection
            System.out.println("Connected to the database successfully!");
        } catch (SQLException e) {
            System.out.println("Error connecting to the database:");
            e.printStackTrace();
        }
    }

    public Connection getConnection() { // Return Connection
        return conn;
    }

    public void close() { // Close Connection
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
                System.out.println("Connection closed."); // Avoid memory leakage
            }
        } catch (SQLException e) {
            System.out.println("Error closing the connection:");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // Create an instance of TestConnection
        TestConnection db = new TestConnection();

        // Test the connection
        if (db.getConnection() != null) {
            System.out.println("Database connection is active and working!");
        } else {
            System.out.println("Failed to establish the database connection.");
        }

        // Close the connection
        db.close();
    }
}
