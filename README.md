# demo
I have,
public interface IPriceRecord {
    String getSubscriberNumber();
    String getCompanyName();
    String getCustomerCountry();
    String getProductName();
    String getCaseDunsNumber();
    String getCaseName();
    String getCaseState();
    String getCaseCountry();
    String getCaseRegion();
    String getReportType();
    LocalDate getBusinessDate();
    String getTransactionType();
    Double getTransactionPrice();
    String getReason();
    Double getListPrice();
    String getAssetLineItemId();
    String getOrderNumber();
    String getCustomerReference();
    String getInvoiceNumber();
    String getHistoricalDate();
    Integer getPortfolioSize();
}
and 
public interface OveragePriceRepository extends CrudRepository<OveragePrice, Long> {
    String OVERAGE_TABLE = "rum_overage_price";
    String REPORT_SELECT_WHERE = "select t.subscriberNumber AS subscriberNumber, "
            + "SUM(t.transactionPrice) AS transactionPrice, " + "MAX(t.companyName) AS companyName, "
            + "MAX(t.subscriberDunsNumber) AS subscriberDunsNumber, " + "t.productName AS productName, "
            + "MAX(t.customerCountry) AS customerCountry " + "from  "
            + "(select t1.subscriber_number as subscriberNumber, " + " t1.transaction_price as transactionPrice,"
            + " t1.company_name as companyName," + " t1.subscriber_duns_number as subscriberDunsNumber,"
            + " t1.product_name as productName," + " t1.customer_country as customerCountry,"
            + " t1.overage_type as overageType, " + " t1.created_date as createdDate " + " from " + OVERAGE_TABLE
            + " t1 where t1.created_date between ?1 and ?2 and t1.is_test_subscriber='N') t ";

    String ORDER_BY_SELECT = " order by t.createdDate";
    String REPORT_GROUP_BY = " GROUP by t.subscriberNumber, " + "t.productName";
    String DAILY_SELECT1 = "select  t.subscriberNumber AS subscriberNumber, " + "t.companyName AS companyName, "
            + "t.customerCountry AS customerCountry, " + "t.productName AS productName, "
            + "t.caseDunsNumber AS caseDunsNumber, " + "t.caseName AS caseName, " + "t.caseState AS caseState, "
            + "t.caseCountry AS caseCountry, " + "t.reportType AS reportType, " + "t.businessDate AS businessDate, "
            + "t.transactionType AS transactionType, " + "t.transactionPrice AS transactionPrice, "
            + "t.caseRegion AS caseRegion, " + "t.reason AS reason, " + "t.listPrice AS listPrice, "
            + "t.assetLineItemId AS assetLineItemId, " + "t.orderNumber AS orderNumber, "
            + "t.customerReference AS customerReference, " + "t.invoiceNumber AS invoiceNumber, "
            + "t.historicalDate AS historicalDate, " + "t.portfolioSize AS portfolioSize " + "from  "
            + "(select  t1.subscriber_number AS subscriberNumber, " + "t1.company_name AS companyName, "
            + "t1.customer_country AS customerCountry, " + "t1.product_name AS productName, "
            + "t1.case_duns_number AS caseDunsNumber, " + "t1.case_name AS caseName, " + "t1.case_state AS caseState, "
            + "t1.case_country AS caseCountry, " + "CASE "
            + "    WHEN locate('wallet', lower(t1.report_type))>0 THEN t1.entitlement_name" + "    ELSE t1.report_type "
            + "END AS reportType, " + "t1.business_date AS businessDate, " + "t1.transaction_type AS transactionType, "
            + "t1.transaction_price AS transactionPrice, " + "t1.case_region AS caseRegion, " + "t1.reason AS reason, "
            + "t1.list_price AS listPrice, " + "t1.asset_line_item_id AS assetLineItemId, "
            + "t1.order_number AS orderNumber, " + "t1.overage_type AS overageType, "
            + "t1.created_date AS createdDate, " + "t1.customer_reference AS customerReference, "
            + "t1.is_test_subscriber AS isTestSubscriber, " + "t1.invoice_number AS invoiceNumber, "
            + "t1.historical_date AS historicalDate, " + "t1.portfolio_size AS portfolioSize " + " from "
            + OVERAGE_TABLE + " t1 where ";

    String DAILY_SELECT2 = " t1.created_date between ?1 and ?2 union all "
            + "select   t2.subscriber_number AS subscriberNumber, " + "t2.company_name AS companyName, "
            + "t2.customer_country AS customerCountry, " + "t2.product_name AS productName, "
            + "t2.case_duns_number AS caseDunsNumber, " + "t2.case_name AS caseName, " + "t2.case_state AS caseState, "
            + "t2.case_country AS caseCountry, " + "CASE "
            + "    WHEN locate('wallet', lower(t2.report_type))>0 THEN t2.entitlement_name" + "    ELSE t2.report_type "
            + "END AS reportType, " + "t2.business_date AS businessDate, " + "t2.transaction_type AS transactionType, "
            + "t2.transaction_price AS transactionPrice, " + "t2.case_region AS caseRegion, " + "t2.reason AS reason, "
            + "t2.list_price AS listPrice, " + "t2.asset_line_item_id AS assetLineItemId, "
            + "t2.order_number AS orderNumber, " + "t2.overage_type AS overageType, "
            + "t2.created_date AS createdDate, " + "t2.customer_reference AS customerReference, "
            + "t2.is_test_subscriber AS isTestSubscriber, " + "t2.invoice_number AS invoiceNumber, "
            + "t2.historical_date AS historicalDate, " + "t2.portfolio_size AS portfolioSize " + " from "
            + METERED_TABLE + " t2 " + " where (t2.report_type in ( "
            + "'Research', 'Comprehensive Report', 'Business Information Report', "
            + "'Enhanced RPS', 'Enhanced RPS Plus', 'Full Research', "
            + "'Mini Research', 'Targeted Research','NI Source of Wealth','NI Resolved Role Player','IP Intelligence','D&B Cyber Compliance', "
            + "'Business Fraud Risk Insight', 'D&B Restricted Party Screening', 'NI Resolved Role Player with SDN', 'Email Verification','Comply Enhanced RPS','Comply Enhanced RPS Plus','D&B Standard Blended Score', 'SBFE Blended Score','SBRI Blended Scores', 'Alternative Supplier AddOn', 'Ad Hoc Screening', 'Comply Ad Hoc Screening' ,'Fraud Risk Insights', 'Outreach Email Invites', "
            + "'Cyber Compliance RUM Count', "
            + "'Protect Illumination AddOn') or lower(t2.transaction_type) like '%wallet%')"
            + " and t2.created_date between ?1 and ?2 ) t" + " WHERE t.createdDate between ?1 and ?2 ";

    String DAILY_SELECT_WHERE = DAILY_SELECT1 + DAILY_SELECT2;

    @QueryHints(value = @QueryHint(name = HINT_FETCH_SIZE, value = "" + Integer.MIN_VALUE))
    @Query(nativeQuery = true, value = DAILY_SELECT_WHERE + "and t.overageType = 'apttus' and t.isTestSubscriber='N'"
            + ORDER_BY_SELECT)
    Stream<IPriceRecord> streamApttusRecords(LocalDateTime startDate, LocalDateTime endDate);

    @QueryHints(value = @QueryHint(name = HINT_FETCH_SIZE, value = "" + Integer.MIN_VALUE))
    @Query(nativeQuery = true, value = DAILY_SELECT_WHERE + "and t.customerCountry in (?3) and t.isTestSubscriber='N'"
            + ORDER_BY_SELECT)
    Stream<IPriceRecord> streamRecords(LocalDateTime startDate, LocalDateTime endDate, List<String> countryies);
public interface MeteredPriceRepository extends CrudRepository<MeteredPrice, Long> {

    and String METERED_TABLE = "rum_metered_price";

    @QueryHints(value = @QueryHint(name = HINT_FETCH_SIZE, value = "" + Integer.MIN_VALUE))
    @Query(value = "select t from MeteredPrice t WHERE t.isTestSubscriber='N' and t.createdDate between ?1 and ?2 order by t.createdDate")
    Stream<MeteredPrice> streamAll(LocalDateTime startDate, LocalDateTime endDate);
    Also, I have,
    public List<Object> getAllRowsByColumnFamily() {
		List<Object> result = new ArrayList<>();
		List<Pricing> pricingList = new ArrayList<>();
		List<Metering> meterList = new ArrayList<>();

		Filter pricingFilter = Filters.FILTERS.family().exactMatch("pricing");
		Filter meteringFilter = Filters.FILTERS.family().exactMatch("metering");
		// Filters.Filter combinedFilter = Filters.FILTERS.interleave().filter(pricingFilter).filter(meteringFilter);

		// Create a query to read rows
		Query query = Query.create(TABLE_NAME).filter(pricingFilter).limit(5);

		// Read rows from the table
		for (Row row : bigtableDataClient.readRows(query)) {
			System.out.println("Row: " + row.getKey().toStringUtf8());
			for (RowCell cell : row.getCells()) {
				System.out.printf("Family: %s    Qualifier: %s    Value: %s%n", cell.getFamily(),
						cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8());
			}
			pricingList.add(Pricing.fromRow(row));
		}

		// Create a query to read rows
		Query query2 = Query.create(TABLE_NAME).filter(meteringFilter).limit(5);

		// Read rows from the table
		for (Row row : bigtableDataClient.readRows(query2)) {
			System.out.println("Row: " + row.getKey().toStringUtf8());
			for (RowCell cell : row.getCells()) {
				System.out.printf("Family: %s    Qualifier: %s    Value: %s%n", cell.getFamily(),
						cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8());
			}
			meterList.add(Metering.fromRow(row));
		}

		result.add(pricingList);
		result.add(meterList);

		return result;
	}
Now I want to replace this with Bigtable query where OVERAGE_TABLE is pricing col family and METERED_TABLE is metering col family of my bigtable.

SQL QUERY:
select  t.subscriberNumber AS subscriberNumber,  t.companyName AS companyName, t.customerCountry AS customerCountry, t.productName AS productName, t.caseDunsNumber AS caseDunsNumber,  t.caseName AS caseName,  t.caseState AS caseState, t.caseCountry AS caseCountry,  t.reportType AS reportType,  t.businessDate AS businessDate, t.transactionType AS transactionType,  t.transactionPrice AS transactionPrice, t.caseRegion AS caseRegion,  t.reason AS reason,  t.listPrice AS listPrice, t.assetLineItemId AS assetLineItemId,  t.orderNumber AS orderNumber, t.customerReference AS customerReference,  t.invoiceNumber AS invoiceNumber, t.historicalDate AS historicalDate,  t.portfolioSize AS portfolioSize  from  ( select  t1.subscriber_number AS subscriberNumber,  t1.company_name AS companyName, t1.customer_country AS customerCountry,  t1.product_name AS productName, t1.case_duns_number AS caseDunsNumber,  t1.case_name AS caseName,  t1.case_state AS caseState, t1.case_country AS caseCountry,  CASE     WHEN locate('wallet', lower(t1.report_type))>0 THEN t1.entitlement_name    ELSE t1.report_type END AS reportType,  t1.business_date AS businessDate,  t1.transaction_type AS transactionType, t1.transaction_price AS transactionPrice,  t1.case_region AS caseRegion,  t1.reason AS reason, t1.list_price AS listPrice,  t1.asset_line_item_id AS assetLineItemId, t1.order_number AS orderNumber,  t1.overage_type AS overageType, t1.created_date AS createdDate,  t1.customer_reference AS customerReference, t1.is_test_subscriber AS isTestSubscriber,  t1.invoice_number AS invoiceNumber, t1.historical_date AS historicalDate,  t1.portfolio_size AS portfolioSize from vbo_usage.rum_overage_price t1 where t1.created_date between '2024-03-01T10:15:30' and '2024-03-05T10:15:30' union all select   t2.subscriber_number AS subscriberNumber,  t2.company_name AS companyName, t2.customer_country AS customerCountry,  t2.product_name AS productName, t2.case_duns_number AS caseDunsNumber,  t2.case_name AS caseName,  t2.case_state AS caseState, t2.case_country AS caseCountry,  CASE     WHEN locate('wallet', lower(t2.report_type))>0 THEN t2.entitlement_name    ELSE t2.report_type END AS reportType,  t2.business_date AS businessDate,  t2.transaction_type AS transactionType, t2.transaction_price AS transactionPrice,  t2.case_region AS caseRegion,  t2.reason AS reason, t2.list_price AS listPrice,  t2.asset_line_item_id AS assetLineItemId, t2.order_number AS orderNumber,  t2.overage_type AS overageType, t2.created_date AS createdDate,  t2.customer_reference AS customerReference, t2.is_test_subscriber AS isTestSubscriber,  t2.invoice_number AS invoiceNumber, t2.historical_date AS historicalDate,  t2.portfolio_size AS portfolioSize   from vbo_usage.rum_metered_price t2   where (t2.report_type in ( 'Research', 'Comprehensive Report', 'Business Information Report', 'Enhanced RPS', 'Enhanced RPS Plus', 'Full Research', 'Mini Research', 'Targeted Research','NI Source of Wealth','NI Resolved Role Player','IP Intelligence','D&B Cyber Compliance', 'Business Fraud Risk Insight', 'D&B Restricted Party Screening', 'NI Resolved Role Player with SDN', 'Email Verification','Comply Enhanced RPS','Comply Enhanced RPS Plus','D&B Standard Blended Score', 'SBFE Blended Score','SBRI Blended Scores', 'Alternative Supplier AddOn', 'Ad Hoc Screening', 'Comply Ad Hoc Screening' ,'Fraud Risk Insights', 'Outreach Email Invites', 'Cyber Compliance RUM Count', 'Protect Illumination AddOn') or lower(t2.transaction_type) like '%wallet%') and t2.created_date between '2024-03-01T10:15:30' and '2024-03-05T10:15:30' ) t WHERE t.createdDate between '2024-03-01T10:15:30' and '2024-03-05T10:15:30' and t.overageType = 'apttus' and t.isTestSubscriber='N' order by t.createdDate;
Explain working or logic of this query and generate same logic for getting same data from Bigtable instead.
