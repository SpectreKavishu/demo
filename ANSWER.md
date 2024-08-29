#Solution
Let's break down the process of converting your MySQL-based logic to work with Google Cloud Bigtable. We'll focus on the complete code for querying Bigtable, including the conversion of the `streamApttusRecords`, `streamRecords`, and `streamAll` methods.

### Prerequisites
- **Bigtable Client Setup:** Ensure you have the Bigtable client properly set up.
- **Table Name:** Define the Bigtable table name.
- **Column Families:** Identify the column families (`pricing` and `metering`) in Bigtable that correspond to your MySQL tables.
- **Data Model Classes:** You should have `Pricing` and `Metering` classes that implement the `fromRow(Row row)` method to map Bigtable rows to your data model.

### 1. Define Bigtable Query Logic

The query logic in Bigtable is different from SQL. We'll create filters based on the required conditions.

#### Stream Apttus Records

```java
public Stream<IPriceRecord> streamApttusRecords(LocalDateTime startDate, LocalDateTime endDate) {
    List<Filter> filters = new ArrayList<>();

    // Date filter
    Filter dateFilter = Filters.FILTERS.timestamp().range().start(MILLISECONDS_OF_EPOCH(startDate)).end(MILLISECONDS_OF_EPOCH(endDate));
    filters.add(dateFilter);

    // Overage Type filter
    Filter overageTypeFilter = Filters.FILTERS.qualifier().exactMatch("overageType");
    filters.add(Filters.FILTERS.chain().filter(overageTypeFilter).filter(Filters.FILTERS.value().exactMatch("apttus")));

    // Test Subscriber filter
    Filter testSubscriberFilter = Filters.FILTERS.qualifier().exactMatch("isTestSubscriber");
    filters.add(Filters.FILTERS.chain().filter(testSubscriberFilter).filter(Filters.FILTERS.value().exactMatch("N")));

    // Combine all filters
    Filter combinedFilter = Filters.FILTERS.chain().filter(filters);

    // Apply the filter to the query
    Query query = Query.create(TABLE_NAME).filter(combinedFilter);

    List<IPriceRecord> results = new ArrayList<>();

    // Execute the query and convert results
    for (Row row : bigtableDataClient.readRows(query)) {
        results.add(Pricing.fromRow(row)); // Assuming `Pricing` implements `IPriceRecord`
    }

    return results.stream();
}
```

#### Stream Records by Country

```java
public Stream<IPriceRecord> streamRecords(LocalDateTime startDate, LocalDateTime endDate, List<String> countries) {
    List<Filter> filters = new ArrayList<>();

    // Date filter
    Filter dateFilter = Filters.FILTERS.timestamp().range().start(MILLISECONDS_OF_EPOCH(startDate)).end(MILLISECONDS_OF_EPOCH(endDate));
    filters.add(dateFilter);

    // Country filter
    Filter countryFilter = Filters.FILTERS.chain();
    for (String country : countries) {
        countryFilter = countryFilter.filter(Filters.FILTERS.value().exactMatch(country));
    }
    filters.add(countryFilter);

    // Test Subscriber filter
    Filter testSubscriberFilter = Filters.FILTERS.qualifier().exactMatch("isTestSubscriber");
    filters.add(Filters.FILTERS.chain().filter(testSubscriberFilter).filter(Filters.FILTERS.value().exactMatch("N")));

    // Combine all filters
    Filter combinedFilter = Filters.FILTERS.chain().filter(filters);

    // Apply the filter to the query
    Query query = Query.create(TABLE_NAME).filter(combinedFilter);

    List<IPriceRecord> results = new ArrayList<>();

    // Execute the query and convert results
    for (Row row : bigtableDataClient.readRows(query)) {
        results.add(Pricing.fromRow(row)); // Assuming `Pricing` implements `IPriceRecord`
    }

    return results.stream();
}
```

#### Stream All Records

```java
public Stream<MeteredPrice> streamAll(LocalDateTime startDate, LocalDateTime endDate) {
    List<Filter> filters = new ArrayList<>();

    // Date filter
    Filter dateFilter = Filters.FILTERS.timestamp().range().start(MILLISECONDS_OF_EPOCH(startDate)).end(MILLISECONDS_OF_EPOCH(endDate));
    filters.add(dateFilter);

    // Test Subscriber filter
    Filter testSubscriberFilter = Filters.FILTERS.qualifier().exactMatch("isTestSubscriber");
    filters.add(Filters.FILTERS.chain().filter(testSubscriberFilter).filter(Filters.FILTERS.value().exactMatch("N")));

    // Combine all filters
    Filter combinedFilter = Filters.FILTERS.chain().filter(filters);

    // Apply the filter to the query
    Query query = Query.create(TABLE_NAME).filter(combinedFilter);

    List<MeteredPrice> results = new ArrayList<>();

    // Execute the query and convert results
    for (Row row : bigtableDataClient.readRows(query)) {
        results.add(Metering.fromRow(row)); // Assuming `Metering` implements `MeteredPrice`
    }

    return results.stream();
}
```

### 2. Utility Method for Timestamp Conversion

You'll need a utility method to convert `LocalDateTime` to milliseconds of epoch (Bigtable uses this format):

```java
private long MILLISECONDS_OF_EPOCH(LocalDateTime dateTime) {
    return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli() * 1000; // Convert to microseconds
}
```

### 3. Mapping Bigtable Rows to Data Classes

Your `Pricing` and `Metering` classes should implement a `fromRow` method to map Bigtable rows to your data model:

```java
public class Pricing implements IPriceRecord {
    public static Pricing fromRow(Row row) {
        // Implement the mapping logic here
    }
}

public class Metering {
    public static Metering fromRow(Row row) {
        // Implement the mapping logic here
    }
}
```

### Conclusion

This is the complete conversion of your MySQL-based logic to Google Cloud Bigtable, covering the key aspects like filtering by dates, subscriber status, and country, and streaming the results. You'll need to adjust the `fromRow` mapping logic based on your exact schema and ensure that the filters match the exact structure of your Bigtable data.


### SQL Query Explanation

**Objective:**
The SQL query retrieves and combines data from two tables, `vbo_usage.rum_overage_price` and `vbo_usage.rum_metered_price`, based on several conditions, and filters the combined results further.

**Query Breakdown:**

1. **Inner Queries:**
   - **First Inner Query (`t1`):**
     - Retrieves columns from the `vbo_usage.rum_overage_price` table.
     - Filters records where `created_date` is between `'2024-03-01T10:15:30'` and `'2024-03-05T10:15:30'`.
     - Applies a transformation to `report_type` to replace it with `entitlement_name` if the report type contains 'wallet'.

   - **Second Inner Query (`t2`):**
     - Retrieves columns from the `vbo_usage.rum_metered_price` table.
     - Filters records based on specific values in `report_type` or if `transaction_type` contains 'wallet'.
     - Further filters records where `created_date` is between `'2024-03-01T10:15:30'` and `'2024-03-05T10:15:30'`.

2. **Union of Results:**
   - Combines the results from the two inner queries using `UNION ALL`.

3. **Final Filtering and Sorting:**
   - Filters the combined results to include only rows where `createdDate` is between `'2024-03-01T10:15:30'` and `'2024-03-05T10:15:30'`, `overageType` is `'apttus'`, and `isTestSubscriber` is `'N'`.
   - Orders the final results by `createdDate`.

### Bigtable Equivalent

Bigtable does not support SQL-like queries directly. Instead, you perform operations like filtering, scanning, and data retrieval programmatically. Here’s how you can achieve similar logic using Bigtable:

1. **Define Filters:**
   - Filter rows based on `created_date` timestamp.
   - Apply specific filters for `report_type` and `transaction_type`.
   - Combine results from two column families.

2. **Combine Results:**
   - Union the results from different column families.

### Example Bigtable Code (Java):

```java
import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class BigtableQueryExample {

    public static void main(String[] args) {
        try (BigtableDataClient dataClient = BigtableDataClient.create("your-project-id", "your-instance-id")) {

            String tableId = "your-table-id";
            String columnFamily1 = "rum_overage_price";
            String columnFamily2 = "rum_metered_price";

            // Define filters
            Filters.Filter createdDateFilter = Filters.FILTERS
                    .chain()
                    .filter(Filters.FILTERS.family().exactMatch(columnFamily1))
                    .filter(Filters.FILTERS.qualifier().exactMatch("created_date"))
                    .filter(Filters.FILTERS.value().range().startClosed("2024-03-01T10:15:30").endClosed("2024-03-05T10:15:30"));

            Filters.Filter reportTypeFilter = Filters.FILTERS
                    .chain()
                    .filter(Filters.FILTERS.family().exactMatch(columnFamily1))
                    .filter(Filters.FILTERS.qualifier().exactMatch("report_type"))
                    .filter(Filters.FILTERS.value().regex(".*wallet.*"));

            Filters.Filter transactionTypeFilter = Filters.FILTERS
                    .chain()
                    .filter(Filters.FILTERS.family().exactMatch(columnFamily2))
                    .filter(Filters.FILTERS.qualifier().exactMatch("transaction_type"))
                    .filter(Filters.FILTERS.value().regex(".*wallet.*"));

            Filters.Filter combinedFilter = Filters.FILTERS
                    .chain()
                    .filter(createdDateFilter)
                    .filter(Filters.FILTERS.or().filter(reportTypeFilter).filter(transactionTypeFilter))
                    .filter(Filters.FILTERS.family().exactMatch(columnFamily2)) // Adjust as needed
                    .filter(Filters.FILTERS.qualifier().exactMatch("overage_type").value().exactMatch("apttus"))
                    .filter(Filters.FILTERS.qualifier().exactMatch("is_test_subscriber").value().exactMatch("N"));

            // Query for Bigtable
            Query query = Query.create(tableId).filter(combinedFilter);

            // Execute the query asynchronously
            ApiFuture<List<Row>> futureRows = dataClient.readRowsCallable().all().futureCall(query);

            // Process the result
            List<Row> rows = futureRows.get();  // Blocking call
            for (Row row : rows) {
                System.out.println("Row key: " + row.getKey().toStringUtf8());
                for (RowCell cell : row.getCells()) {
                    System.out.printf("Family: %s, Qualifier: %s, Value: %s%n",
                        cell.getFamily(), cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8());
                }
            }

        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### Key Points:
- **Filters**: Define filters based on column family, qualifiers, and values to mimic SQL conditions.
- **Combining Results**: Use `Filters.FILTERS.or()` to handle conditions that would be combined using `UNION ALL` in SQL.
- **Timestamp Range**: Ensure that the timestamp range in the filters is in the correct format.

Adjust the code according to your specific Bigtable setup, including column families and qualifiers.
