#data

For the given Mysql query, change my java code considering rum_metered_price is equivalent to 'metering' column family and 'pricing' is equivalent to rum_overage_price.

query: select t.subscriberNumber AS subscriberNumber,  t.companyName AS companyName, t.customerCountry AS customerCountry, t.productName AS productName, t.caseDunsNumber AS caseDunsNumber,  t.caseName AS caseName,  t.caseState AS caseState, t.caseCountry AS caseCountry,  t.reportType AS reportType,  t.businessDate AS businessDate, t.transactionType AS transactionType,  t.transactionPrice AS transactionPrice, t.caseRegion AS caseRegion,  t.reason AS reason,  t.listPrice AS listPrice, t.assetLineItemId AS assetLineItemId,  t.orderNumber AS orderNumber, t.customerReference AS customerReference,  t.invoiceNumber AS invoiceNumber, t.historicalDate AS historicalDate,  t.portfolioSize AS portfolioSize  from  (select  t1.subscriber_number AS subscriberNumber,  t1.company_name AS companyName, t1.customer_country AS customerCountry,  t1.product_name AS productName, t1.case_duns_number AS caseDunsNumber,  t1.case_name AS caseName,  t1.case_state AS caseState, t1.case_country AS caseCountry,  CASE     WHEN locate('wallet', lower(t1.report_type))>0 THEN t1.entitlement_name    ELSE t1.report_type END AS reportType,  t1.business_date AS businessDate,  t1.transaction_type AS transactionType, t1.transaction_price AS transactionPrice,  t1.case_region AS caseRegion,  t1.reason AS reason, t1.list_price AS listPrice,  t1.asset_line_item_id AS assetLineItemId, t1.order_number AS orderNumber,  t1.overage_type AS overageType, t1.created_date AS createdDate,  t1.customer_reference AS customerReference, t1.is_test_subscriber AS isTestSubscriber,  t1.invoice_number AS invoiceNumber, t1.historical_date AS historicalDate,  t1.portfolio_size AS portfolioSize from vbo_usage.rum_overage_price t1 where t1.created_date between '2024-03-01T10:15:30' and '2024-03-05T10:15:30' union all select   t2.subscriber_number AS subscriberNumber,  t2.company_name AS companyName, t2.customer_country AS customerCountry,  t2.product_name AS productName, t2.case_duns_number AS caseDunsNumber,  t2.case_name AS caseName,  t2.case_state AS caseState, t2.case_country AS caseCountry,  CASE     WHEN locate('wallet', lower(t2.report_type))>0 THEN t2.entitlement_name    ELSE t2.report_type END AS reportType,  t2.business_date AS businessDate,  t2.transaction_type AS transactionType, t2.transaction_price AS transactionPrice,  t2.case_region AS caseRegion,  t2.reason AS reason, t2.list_price AS listPrice,  t2.asset_line_item_id AS assetLineItemId, t2.order_number AS orderNumber,  t2.overage_type AS overageType, t2.created_date AS createdDate,  t2.customer_reference AS customerReference, t2.is_test_subscriber AS isTestSubscriber,  t2.invoice_number AS invoiceNumber, t2.historical_date AS historicalDate,  t2.portfolio_size AS portfolioSize   from vbo_usage.rum_metered_price t2   where (t2.report_type in ( 'Research', 'Comprehensive Report', 'Business Information Report', 'Enhanced RPS', 'Enhanced RPS Plus', 'Full Research', 'Mini Research', 'Targeted Research','NI Source of Wealth','NI Resolved Role Player','IP Intelligence','D&B Cyber Compliance', 'Business Fraud Risk Insight', 'D&B Restricted Party Screening', 'NI Resolved Role Player with SDN', 'Email Verification','Comply Enhanced RPS','Comply Enhanced RPS Plus','D&B Standard Blended Score', 'SBFE Blended Score','SBRI Blended Scores', 'Alternative Supplier AddOn', 'Ad Hoc Screening', 'Comply Ad Hoc Screening' ,'Fraud Risk Insights', 'Outreach Email Invites', 'Cyber Compliance RUM Count', 'Protect Illumination AddOn') or lower(t2.transaction_type) like '%wallet%') and t2.created_date between '2024-03-01T10:15:30' and '2024-03-05T10:15:30' ) t WHERE t.createdDate between '2024-03-01T10:15:30' and '2024-03-05T10:15:30' and t.overageType = 'apttus' and t.isTestSubscriber='N' order by t.createdDate;
java code: package com.dnb.vbo.pricing;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
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

		// Scan for the first subquery
		Scan scan1 = new Scan();
		scan1.addFamily(Bytes.toBytes("metering"));
		scan1.setFilter(createFilter("2024-03-01T10:15:30", "2024-03-05T10:15:30", "apttus", "N", "metering"));

		ResultScanner scanner1 = table.getScanner(scan1);
		List<Result> results1 = new ArrayList<>();
		for (Result result : scanner1) {
			results1.add(result);
		}

		// Scan for the second subquery
		Scan scan2 = new Scan();
		scan2.addFamily(Bytes.toBytes("pricing"));
		scan2.setFilter(createFilter("2024-03-01T10:15:30", "2024-03-05T10:15:30", "apttus", "N", "pricing"));

		ResultScanner scanner2 = table.getScanner(scan2);
		List<Result> results2 = new ArrayList<>();
		for (Result result : scanner2) {
			results2.add(result);
		}

		System.out.println("-- RESULTS --");
		System.out.println(results1);
		System.out.println(results2);
		System.out.println("-- END --");

		// Merge results
		List<Result> mergedResults = new ArrayList<>();
		mergedResults.addAll(results1);
		mergedResults.addAll(results2);

		// Sort results by createdDate (you'll need to implement this)
		mergedResults.sort((r1, r2) -> {
			String date1 = Bytes.toString(r1.getValue(Bytes.toBytes("pricing"), Bytes.toBytes("created_date")));
			String date2 = Bytes.toString(r2.getValue(Bytes.toBytes("pricing"), Bytes.toBytes("created_date")));
			// return date1.compareTo(date2);
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
		System.out.println("-- CONNECTION OVER --");
	}

	private static Filter createFilter(String startDate, String endDate, String overageType, String isTestSubscriber,
			String colFam) {
		List<Filter> filters = new ArrayList<>();
		filters.add(new SingleColumnValueFilter(Bytes.toBytes(colFam), Bytes.toBytes("created_date"),
				CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startDate)));
		filters.add(new SingleColumnValueFilter(Bytes.toBytes(colFam), Bytes.toBytes("created_date"),
				CompareOp.LESS_OR_EQUAL, Bytes.toBytes(endDate)));
		filters.add(new SingleColumnValueFilter(Bytes.toBytes(colFam), Bytes.toBytes("overage_type"), CompareOp.EQUAL,
				Bytes.toBytes(overageType)));
		filters.add(new SingleColumnValueFilter(Bytes.toBytes(colFam), Bytes.toBytes("is_test_subscriber"),
				CompareOp.EQUAL, Bytes.toBytes(isTestSubscriber)));
		return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
	}
}
Note: Data is already there in Bigtable with all the qualifiers same as mysql tables. I just need the corrected and complete java code.
