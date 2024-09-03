Change below code so that it returns results as Stream<IPriceRecord> within function named as streamApttusRecords(LocalDateTime startDate, LocalDateTime endDate)

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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import java.util.ArrayList;
import java.util.List;

public class Test2 {
	public static void main(String[] args) throws Exception {
		String projectId = "nprd-vbo-devd634cab9b2279d7de1";
		String instanceId = "vbo-usage-instance";
		String tableId = "vbo-usage-raw-data";

		Connection connection = BigtableConfiguration.connect(projectId, instanceId);
		Table table = connection.getTable(TableName.valueOf(tableId));

		// Scan for rum_metered_price (metering column family)
		Scan meteringScan = new Scan();
		meteringScan.addFamily(Bytes.toBytes("metering"));
		meteringScan.setFilter(createMeteringFilter());

		ResultScanner meteringScanner = table.getScanner(meteringScan);
		List<Result> meteringResults = new ArrayList<>();
		for (Result result : meteringScanner) {
			meteringResults.add(result);
		}

		// Scan for rum_overage_price (pricing column family)
		Scan pricingScan = new Scan();
		pricingScan.addFamily(Bytes.toBytes("pricing"));
		pricingScan.setFilter(createPricingFilter());

		ResultScanner pricingScanner = table.getScanner(pricingScan);
		List<Result> pricingResults = new ArrayList<>();
		for (Result result : pricingScanner) {
			pricingResults.add(result);
		}

		// Merge and sort results by createdDate
		List<Result> mergedResults = new ArrayList<>();
		mergedResults.addAll(meteringResults);
		mergedResults.addAll(pricingResults);

		mergedResults.sort((r1, r2) -> {
			String date1 = Bytes.toString(r1.getValue(Bytes.toBytes("metering"), Bytes.toBytes("created_date")));
			String date2 = Bytes.toString(r2.getValue(Bytes.toBytes("pricing"), Bytes.toBytes("created_date")));
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
			String subscriberNumber = getValueOrNull(result, "metering", "subscriber_number");
			if (subscriberNumber == null) {
				subscriberNumber = getValueOrNull(result, "pricing", "subscriber_number");
			}
			String companyName = getValueOrNull(result, "metering", "company_name");
			if (companyName == null) {
				companyName = getValueOrNull(result, "pricing", "company_name");
			}
			String caseDuns = getValueOrNull(result, "metering", "case_duns_number");
			if (caseDuns == null) {
				caseDuns = getValueOrNull(result, "pricing", "case_duns_number");
			}

			// Retrieve and print other columns similarly...
			System.out.println("Subscriber Number: " + subscriberNumber);
			System.out.println("Company Name: " + companyName);
			System.out.println("Case Duns: " + caseDuns);
			// Print other columns similarly...
		}

		connection.close();
	}

	// Utility method to get value from a column family or return null if not present
	private static String getValueOrNull(Result result, String colFamily, String qualifier) {
		byte[] value = result.getValue(Bytes.toBytes(colFamily), Bytes.toBytes(qualifier));
		return value != null ? Bytes.toString(value) : null;
	}

	private static Filter createMeteringFilter() {
		List<Filter> filters = new ArrayList<>();
		filters.add(new SingleColumnValueFilter(Bytes.toBytes("metering"), Bytes.toBytes("created_date"),
				CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("2024-03-01 10:15:30")));
		filters.add(new SingleColumnValueFilter(Bytes.toBytes("metering"), Bytes.toBytes("created_date"),
				CompareOp.LESS_OR_EQUAL, Bytes.toBytes("2024-03-05 10:15:30")));
		filters.add(new SingleColumnValueFilter(Bytes.toBytes("metering"), Bytes.toBytes("is_test_subscriber"),
				CompareOp.EQUAL, Bytes.toBytes("N")));

		// Filtering based on report_type
		List<Filter> reportTypeFilters = new ArrayList<>();
		String[] reportTypes = { "Research", "Comprehensive Report", "Business Information Report", "Enhanced RPS",
				"Enhanced RPS Plus", "Full Research", "Mini Research", "Targeted Research", "NI Source of Wealth",
				"NI Resolved Role Player", "IP Intelligence", "D&B Cyber Compliance", "Business Fraud Risk Insight",
				"D&B Restricted Party Screening", "NI Resolved Role Player with SDN", "Email Verification",
				"Comply Enhanced RPS", "Comply Enhanced RPS Plus", "D&B Standard Blended Score", "SBFE Blended Score",
				"SBRI Blended Scores", "Alternative Supplier AddOn", "Ad Hoc Screening", "Comply Ad Hoc Screening",
				"Fraud Risk Insights", "Outreach Email Invites", "Cyber Compliance RUM Count",
				"Protect Illumination AddOn" };

		for (String reportType : reportTypes) {
			reportTypeFilters.add(new SingleColumnValueFilter(Bytes.toBytes("metering"), Bytes.toBytes("report_type"),
					CompareOp.EQUAL, Bytes.toBytes(reportType)));
		}

		FilterList reportTypeFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, reportTypeFilters);
		filters.add(reportTypeFilterList);

		// Filtering based on transaction_type
		filters.add(new SingleColumnValueFilter(Bytes.toBytes("metering"), Bytes.toBytes("transaction_type"),
				CompareOp.EQUAL, Bytes.toBytes("wallet")));

		return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
	}

	private static Filter createPricingFilter() {
		List<Filter> filters = new ArrayList<>();
		filters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("created_date"),
				CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("2024-03-01 10:15:30")));
		filters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("created_date"),
				CompareOp.LESS_OR_EQUAL, Bytes.toBytes("2024-03-05 10:15:30")));
		filters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("is_test_subscriber"),
				CompareOp.EQUAL, Bytes.toBytes("N")));
		filters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("overage_type"),
				CompareOp.EQUAL, Bytes.toBytes("apttus")));

		return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
	}
}
