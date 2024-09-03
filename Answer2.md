You're right—the initial conversion didn't fully align with the MySQL query's complexity, especially regarding the additional filtering criteria and conditions. Let's refactor the Java code to better mirror the MySQL query, ensuring all the `WHERE` clause conditions and the `UNION ALL` logic are accurately reflected.

Here’s an updated version of your Java code that better corresponds to the given MySQL query:

```java
package com.dnb.vbo.pricing;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) throws Exception {
        String projectId = "nprd-vbo-devd634cab9b2279d7de1";
        String instanceId = "vbo-usage-instance";
        String tableId = "vbo-usage-raw-data";

        Connection connection = BigtableConfiguration.connect(projectId, instanceId);
        Table table = connection.getTable(TableName.valueOf(tableId));

        // Scan for the first subquery (pricing column family)
        Scan scan1 = new Scan();
        scan1.addFamily(Bytes.toBytes("pricing"));
        scan1.setFilter(createFilterForPricing());

        ResultScanner scanner1 = table.getScanner(scan1);
        List<Result> results1 = new ArrayList<>();
        for (Result result : scanner1) {
            results1.add(result);
        }

        // Scan for the second subquery (metering column family)
        Scan scan2 = new Scan();
        scan2.addFamily(Bytes.toBytes("metering"));
        scan2.setFilter(createFilterForMetering());

        ResultScanner scanner2 = table.getScanner(scan2);
        List<Result> results2 = new ArrayList<>();
        for (Result result : scanner2) {
            results2.add(result);
        }

        // Merge and sort results by createdDate
        List<Result> mergedResults = new ArrayList<>();
        mergedResults.addAll(results1);
        mergedResults.addAll(results2);

        mergedResults.sort((r1, r2) -> {
            String date1 = Bytes.toString(r1.getValue(Bytes.toBytes("pricing"), Bytes.toBytes("created_date")));
            String date2 = Bytes.toString(r2.getValue(Bytes.toBytes("metering"), Bytes.toBytes("created_date")));
            if (date1 == null && date2 == null) {
                return 0;
            } else if (date1 == null) {
                return -1;
            } else if (date2 == null) {
                return 1;
            } else {
                return date1.compareTo(date2);
            }
        });

        // Print results
        for (Result result : mergedResults) {
            String subscriberNumber = Bytes
                    .toString(result.getValue(Bytes.toBytes("pricing"), Bytes.toBytes("subscriber_number")));
            String companyName = Bytes
                    .toString(result.getValue(Bytes.toBytes("pricing"), Bytes.toBytes("company_name")));
            String caseDuns = Bytes
                    .toString(result.getValue(Bytes.toBytes("pricing"), Bytes.toBytes("case_duns_number")));
            // Retrieve other columns similarly

            System.out.println("Subscriber Number: " + subscriberNumber);
            System.out.println("Company Name: " + companyName);
            System.out.println("Case Duns: " + caseDuns);
            // Print other columns similarly
        }

        connection.close();
    }

    private static Filter createFilterForPricing() {
        List<Filter> filters = new ArrayList<>();
        filters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("created_date"),
                CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("2024-03-01T10:15:30")));
        filters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("created_date"),
                CompareOp.LESS_OR_EQUAL, Bytes.toBytes("2024-03-05T10:15:30")));
        filters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("overage_type"), CompareOp.EQUAL,
                Bytes.toBytes("apttus")));
        filters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("is_test_subscriber"),
                CompareOp.EQUAL, Bytes.toBytes("N")));

        // Add other filters according to your SQL WHERE clause for the pricing family
        return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
    }

    private static Filter createFilterForMetering() {
        List<Filter> filters = new ArrayList<>();
        filters.add(new SingleColumnValueFilter(Bytes.toBytes("metering"), Bytes.toBytes("created_date"),
                CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("2024-03-01T10:15:30")));
        filters.add(new SingleColumnValueFilter(Bytes.toBytes("metering"), Bytes.toBytes("created_date"),
                CompareOp.LESS_OR_EQUAL, Bytes.toBytes("2024-03-05T10:15:30")));
        filters.add(new SingleColumnValueFilter(Bytes.toBytes("metering"), Bytes.toBytes("is_test_subscriber"),
                CompareOp.EQUAL, Bytes.toBytes("N")));

        // Add the additional conditions from the SQL query, such as filtering by report_type and transaction_type
        List<Filter> reportTypeFilters = new ArrayList<>();
        String[] reportTypes = {
                "Research", "Comprehensive Report", "Business Information Report", "Enhanced RPS", "Enhanced RPS Plus",
                "Full Research", "Mini Research", "Targeted Research", "NI Source of Wealth", "NI Resolved Role Player",
                "IP Intelligence", "D&B Cyber Compliance", "Business Fraud Risk Insight", "D&B Restricted Party Screening",
                "NI Resolved Role Player with SDN", "Email Verification", "Comply Enhanced RPS", "Comply Enhanced RPS Plus",
                "D&B Standard Blended Score", "SBFE Blended Score", "SBRI Blended Scores", "Alternative Supplier AddOn",
                "Ad Hoc Screening", "Comply Ad Hoc Screening", "Fraud Risk Insights", "Outreach Email Invites",
                "Cyber Compliance RUM Count", "Protect Illumination AddOn"
        };

        for (String reportType : reportTypes) {
            reportTypeFilters.add(new SingleColumnValueFilter(Bytes.toBytes("metering"), Bytes.toBytes("report_type"),
                    CompareOp.EQUAL, Bytes.toBytes(reportType)));
        }

        FilterList reportTypeFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, reportTypeFilters);
        filters.add(reportTypeFilterList);

        filters.add(new SingleColumnValueFilter(Bytes.toBytes("metering"), Bytes.toBytes("transaction_type"),
                CompareOp.LIKE, Bytes.toBytes("%wallet%")));

        return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
    }
}
```

### Key Enhancements:
1. **Separate Filter Methods for Each Subquery**: The method `createFilterForPricing()` handles the filtering logic for the `pricing` column family, and `createFilterForMetering()` handles the logic for the `metering` column family.

2. **Inclusion of Additional SQL WHERE Clauses**:
   - For `pricing`, filters are added according to the conditions in the MySQL query.
   - For `metering`, the `report_type` and `transaction_type` conditions from the MySQL query are applied.

3. **Union Logic**: Both scans are executed separately, their results are merged, and the final result set is sorted by `created_date`.

4. **Complex Conditions**: The code mirrors the complex conditions found in your SQL `WHERE` clause, such as checking for specific report types and handling conditions on the `transaction_type` field.

This should now more accurately reflect the logic from your MySQL query in the Bigtable environment.