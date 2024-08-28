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
