import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class MeshJoin {
    private static String DB_URL;
    private static String USER;
    private static String PASSWORD;

    private static BlockingQueue<Transaction> streamBuffer = new LinkedBlockingQueue<>();
    private static List<MasterData> customerMaster = new ArrayList<>();
    private static List<MasterData> productMaster = new ArrayList<>();
    private static Map<Integer, String> supplierData = new HashMap<>();
    private static Map<Integer, String> storeData = new HashMap<>();
    private static final int STREAM_BUFFER_SIZE = 100;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter database URL: jdbc:mysql://localhost:3306/Metro_DW ");
        DB_URL = scanner.nextLine();
        System.out.print("Enter database username: ");
        USER = scanner.nextLine();
        System.out.print("Enter database password: ");
        PASSWORD = scanner.nextLine();
        scanner.close();

        try {
            loadMasterData();
            ExecutorService executor = Executors.newFixedThreadPool(2);
            executor.execute(MeshJoin::processTransactions);
            executor.execute(MeshJoin::streamTransactions);
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            System.out.println("MESHJOIN process completed.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void loadMasterData() {
        try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASSWORD)) {
            loadCustomerData(connection);
            loadProductData(connection);
            loadSupplierData(connection);
            loadStoreData(connection);
            populateTimeDimension(connection);
            System.out.println("All Master Data loaded successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void loadCustomerData(Connection connection) throws SQLException {
        String query = "SELECT customerID, customerName, gender FROM Customer_Dimension";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                customerMaster.add(new MasterData(
                        rs.getInt("customerID"),
                        rs.getString("customerName"),
                        rs.getString("gender"),
                        0, 0));
            }
        }
        System.out.println("Customer Master Data loaded. Records: " + customerMaster.size());
    }

    private static void loadProductData(Connection connection) throws SQLException {
        String query = "SELECT productID, productName, productPrice, supplierID, storeID FROM Dummy_Table";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                productMaster.add(new MasterData(
                        rs.getInt("productID"),
                        rs.getString("productName"),
                        rs.getDouble("productPrice"),
                        rs.getInt("supplierID"),
                        rs.getInt("storeID")));
            }
        }
        System.out.println("Product Master Data loaded. Records: " + productMaster.size());
    }

    private static void loadSupplierData(Connection connection) throws SQLException {
        String query = "SELECT supplierID, supplierName FROM Supplier_Dimension";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                supplierData.put(rs.getInt("supplierID"), rs.getString("supplierName"));
            }
        }
        System.out.println("Supplier Data loaded. Records: " + supplierData.size());
    }

    private static void loadStoreData(Connection connection) throws SQLException {
        String query = "SELECT storeID, storeName FROM Store_Dimension";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                storeData.put(rs.getInt("storeID"), rs.getString("storeName"));
            }
        }
        System.out.println("Store Data loaded. Records: " + storeData.size());
    }

    private static void populateTimeDimension(Connection connection) {
        String query = "INSERT INTO Time_Dimension (timeID, orderDate, orderTime, Day, Month, Quarter, Year) " +
                       "VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE timeID = timeID";

        String csvFile = "X:\\5th Semester\\DW BI\\DW\\transactions_data.csv";
        String line;
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy h:mm a");

        Map<Integer, String> uniqueTimes = new LinkedHashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields.length == 7) {
                    try {
                        int timeID = Integer.parseInt(fields[6]);
                        String orderDateStr = fields[4];
                        String orderTimeStr = fields[5];

                        String dateTimeStr = orderDateStr + " " + orderTimeStr;
                        if (!uniqueTimes.containsKey(timeID)) {
                            uniqueTimes.put(timeID, dateTimeStr);
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("Skipping malformed row due to NumberFormatException: " + line);
                    }
                } else {
                    System.err.println("Skipping malformed row: " + line);
                }
            }

            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                for (Map.Entry<Integer, String> entry : uniqueTimes.entrySet()) {
                    int timeID = entry.getKey();
                    String dateTimeStr = entry.getValue();

                    try {
                        java.util.Date parsedDate = dateFormat.parse(dateTimeStr);
                        java.sql.Date orderDate = new java.sql.Date(parsedDate.getTime());
                        java.sql.Time orderTime = new java.sql.Time(parsedDate.getTime());

                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(orderDate);

                        int day = calendar.get(Calendar.DAY_OF_MONTH);
                        int month = calendar.get(Calendar.MONTH) + 1;
                        int year = calendar.get(Calendar.YEAR);
                        int quarter = (month - 1) / 3 + 1;

                        stmt.setInt(1, timeID);
                        stmt.setDate(2, orderDate);
                        stmt.setTime(3, orderTime);
                        stmt.setInt(4, day);
                        stmt.setInt(5, month);
                        stmt.setInt(6, quarter);
                        stmt.setInt(7, year);
                        stmt.addBatch();
                    } catch (ParseException e) {
                        System.err.println("Skipping entry due to ParseException for timeID " + timeID + ": " + dateTimeStr);
                    }
                }
                stmt.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            System.out.println("Time Dimension populated successfully with " + uniqueTimes.size() + " unique records.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void streamTransactions() {
        String csvFile = "X:\\5th Semester\\DW BI\\DW\\transactions_data.csv";
        String line;
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy h:mm a");
        int recordCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            br.readLine();
            while ((line = br.readLine()) != null) {
                if (streamBuffer.size() >= STREAM_BUFFER_SIZE) {
                    Thread.sleep(100);
                }
                String[] fields = line.split(",");
                if (fields.length == 7) {
                    try {
                        int orderID = Integer.parseInt(fields[0]);
                        int productID = Integer.parseInt(fields[1]);
                        int quantityOrdered = Integer.parseInt(fields[2]);
                        int customerID = Integer.parseInt(fields[3]);
                        String orderDateStr = fields[4];
                        String orderTimeStr = fields[5];
                        int timeID = Integer.parseInt(fields[6]);

                        java.util.Date parsedDate = dateFormat.parse(orderDateStr + " " + orderTimeStr);
                        java.sql.Date orderDate = new java.sql.Date(parsedDate.getTime());

                        streamBuffer.add(new Transaction(orderID, orderDate, productID, quantityOrdered, customerID, timeID));
                        recordCount++;
                    } catch (ParseException e) {
                        System.err.println("Skipping malformed row due to ParseException: " + line);
                    } catch (NumberFormatException e) {
                        System.err.println("Skipping malformed row due to NumberFormatException: " + line);
                    }
                } else {
                    System.err.println("Skipping malformed row: " + line);
                }
            }
            System.out.println("All transactions streamed. Total records: " + recordCount);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void processTransactions() {
        try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASSWORD)) {
            String query = "INSERT INTO Sales_FactTable (orderID, productID, customerID, supplierID, storeID, orderDate, timeID, quantityOrdered, totalSale) " +
                           "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                           "ON DUPLICATE KEY UPDATE quantityOrdered = quantityOrdered + VALUES(quantityOrdered), " +
                           "totalSale = totalSale + VALUES(totalSale)";
            PreparedStatement stmt = connection.prepareStatement(query);

            while (true) {
                Transaction transaction = streamBuffer.poll(10, TimeUnit.SECONDS);
                if (transaction == null) break;

                MasterData product = findMasterData(productMaster, transaction.getProductID());
                if (product != null) {
                    int supplierID = product.getSupplierID();
                    int storeID = product.getStoreID();

                    if (!supplierData.containsKey(supplierID)) {
                        System.err.println("Invalid supplierID " + supplierID + " for productID " + transaction.getProductID());
                        continue;
                    }
                    if (!storeData.containsKey(storeID)) {
                        System.err.println("Invalid storeID " + storeID + " for productID " + transaction.getProductID());
                        continue;
                    }

                    double totalSale = transaction.getQuantityOrdered() * product.getPrice();

                    stmt.setInt(1, transaction.getOrderID());
                    stmt.setInt(2, transaction.getProductID());
                    stmt.setInt(3, transaction.getCustomerID());
                    stmt.setInt(4, supplierID);
                    stmt.setInt(5, storeID);
                    stmt.setDate(6, transaction.getOrderDate());
                    stmt.setInt(7, transaction.getTimeID());
                    stmt.setInt(8, transaction.getQuantityOrdered());
                    stmt.setDouble(9, totalSale);
                    stmt.executeUpdate();
                } else {
                    System.err.println("Product not found for transaction: " + transaction);
                }
            }
            stmt.close();
            System.out.println("Transactions processed successfully.");
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static MasterData findMasterData(List<MasterData> masterList, int id) {
        return masterList.stream().filter(md -> md.getId() == id).findFirst().orElse(null);
    }

    private static class MasterData {
        private int id;
        private String name;
        private Object extraData;
        private int supplierID;
        private int storeID;

        public MasterData(int id, String name, Object extraData, int supplierID, int storeID) {
            this.id = id;
            this.name = name;
            this.extraData = extraData;
            this.supplierID = supplierID;
            this.storeID = storeID;
        }

        public int getId() { return id; }
        public String getName() { return name; }
        public Object getExtraData() { return extraData; }
        public int getSupplierID() { return supplierID; }
        public int getStoreID() { return storeID; }

        public double getPrice() {
            return extraData instanceof Double ? (Double) extraData : 0.0;
        }

        @Override
        public String toString() {
            return "MasterData{" +
                   "id=" + id +
                   ", name='" + name + '\'' +
                   ", extraData=" + extraData +
                   ", supplierID=" + supplierID +
                   ", storeID=" + storeID +
                   '}';
        }
    }

    private static class Transaction {
        private int orderID;
        private java.sql.Date orderDate;
        private int productID;
        private int quantityOrdered;
        private int customerID;
        private int timeID;

        public Transaction(int orderID, java.sql.Date orderDate, int productID, int quantityOrdered, int customerID, int timeID) {
            this.orderID = orderID;
            this.orderDate = orderDate;
            this.productID = productID;
            this.quantityOrdered = quantityOrdered;
            this.customerID = customerID;
            this.timeID = timeID;
        }

        public int getOrderID() { return orderID; }
        public java.sql.Date getOrderDate() { return orderDate; }
        public int getProductID() { return productID; }
        public int getQuantityOrdered() { return quantityOrdered; }
        public int getCustomerID() { return customerID; }
        public int getTimeID() { return timeID; }

        @Override
        public String toString() {
            return "Transaction{" +
                   "orderID=" + orderID +
                   ", orderDate=" + orderDate +
                   ", productID=" + productID +
                   ", quantityOrdered=" + quantityOrdered +
                   ", customerID=" + customerID +
                   ", timeID=" + timeID +
                   '}';
        }
    }
}