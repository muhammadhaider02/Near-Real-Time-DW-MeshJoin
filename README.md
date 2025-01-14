# Near-Real-Time Data Warehouse with MeshJoin Algorithm

This project implements a Near-Real-Time Data Warehouse system using the MeshJoin algorithm for efficient stream processing and data integration.

## 📋 Project Overview

The system processes real-time transaction streams and joins them with master data using the MeshJoin algorithm, providing near-real-time analytics capabilities for a retail data warehouse.

## 🏗️ Architecture

### Star Schema Design
The data warehouse follows a star schema with the following dimensions:
- **Customer Dimension**: Customer information (ID, Name, Gender)
- **Product Dimension**: Product details (ID, Name, Price)
- **Supplier Dimension**: Supplier information (ID, Name)
- **Store Dimension**: Store details (ID, Name)
- **Time Dimension**: Temporal attributes (Date, Time, Day, Month, Quarter, Year)
- **Sales Fact Table**: Central fact table containing sales transactions

### MeshJoin Implementation
- **Stream Buffer**: Processes incoming transaction streams
- **Master Data Loading**: Pre-loads dimension tables into memory
- **Concurrent Processing**: Uses multi-threading for stream and transaction processing
- **Real-time Joins**: Performs efficient joins between streaming data and master data

## 📁 Project Structure

```
Near-Real-Time-DW-MeshJoin/
├── Java-DW/
│   ├── src/
│   │   ├── MeshJoin.java          # Main MeshJoin implementation
│   │   └── TestConnection.java    # Database connection testing
│   └── bin/                       # Compiled Java classes
├── Create-DW.sql                  # Database schema creation
├── Queries-DW.sql                 # Analytical queries
├── customers_data.csv             # Customer dimension data
├── products_data.csv              # Product dimension data
├── transactions_data.csv          # Transaction stream data
├── StarSchema.png                 # Database schema diagram
└── README.md                      # This file
```

## 🚀 Getting Started

### Prerequisites
- Java 8 or higher
- MySQL Database Server
- MySQL Connector/J (JDBC driver)

### Database Setup
1. Create the database and tables:
```sql
mysql -u your_username -p < Create-DW.sql
```

2. Load sample data:
```sql
-- Update file paths in Create-DW.sql to match your data file locations
mysql -u your_username -p Metro_DW < Create-DW.sql
```

### Running the Application
1. Compile the Java source:
```bash
cd Java-DW/src
javac *.java
```

2. Run the MeshJoin application:
```bash
cd Java-DW/bin
java MeshJoin
```

3. Enter database connection details when prompted:
   - Database URL: `jdbc:mysql://localhost:3306/Metro_DW`
   - Username: Your MySQL username
   - Password: Your MySQL password

## 📊 Sample Queries

The `Queries-DW.sql` file contains various analytical queries including:

1. **Top Revenue-Generating Products**: Analysis by weekdays vs weekends with monthly drill-down
2. **Store Revenue Growth Rate**: Quarterly trend analysis for 2019
3. **Customer Segmentation**: Revenue analysis by customer demographics
4. **Product Performance**: Cross-dimensional analysis of product sales

## 🔧 Key Features

- **Real-time Stream Processing**: Handles continuous transaction streams
- **Memory-Efficient Joins**: MeshJoin algorithm optimizes memory usage
- **Multi-threaded Architecture**: Concurrent processing for better performance
- **Star Schema Design**: Optimized for analytical queries
- **Comprehensive Analytics**: Pre-built queries for business intelligence

## 📈 Performance Considerations

- **Stream Buffer Size**: Configurable buffer size for transaction processing
- **Master Data Loading**: Pre-loads dimension tables for faster joins
- **Connection Pooling**: Efficient database connection management
- **Memory Management**: Optimized data structures for large datasets

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

