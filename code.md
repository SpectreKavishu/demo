with this it works:
Filter filter = createPricingFilter(startDate, endDate, "N", countries);

	    Scan scan = new Scan();
	    scan.setFilter(filter);
	    scan.addFamily(Bytes.toBytes("pricing"));

	    ResultScanner scanner = connection.getTable(TableName.valueOf(TABLE_NAME)).getScanner(scan);
     private static Filter createPricingFilter(LocalDateTime startDate, LocalDateTime endDate, String isTestSubscriber,
			List<String> countriesList) {
		List<Filter> filters = new ArrayList<>();
		filters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("created_date"),
				CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startDate.format(DATE_TIME_FORMATTER))));
		filters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("created_date"),
				CompareOp.LESS_OR_EQUAL, Bytes.toBytes(endDate.format(DATE_TIME_FORMATTER))));
		filters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("is_test_subscriber"),
				CompareOp.EQUAL, Bytes.toBytes(isTestSubscriber)));

		// Create a filter list for countries
		List<Filter> countryFilters = new ArrayList<>();
		for (String country : countriesList) {
			countryFilters.add(new SingleColumnValueFilter(Bytes.toBytes("pricing"), Bytes.toBytes("customer_country"),
					CompareOp.EQUAL, Bytes.toBytes(country)));
		}
		filters.add(new FilterList(FilterList.Operator.MUST_PASS_ONE, countryFilters));

		return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
	}
 
 I need to make it also work with:
 Filters.Filter filter = createPricingFilter(startDate, endDate, "N", countries);
			
			Query query = Query.create(TableId.of(TABLE_NAME)).filter(filter);
			
			private static Filters.Filter createPricingFilter(LocalDateTime startDate, LocalDateTime endDate,
			String isTestSubscriber, List<String> countriesList) {
		String columnFamily = "pricing";
		Filters.ChainFilter chainFilter = Filters.FILTERS.chain()
				.filter(Filters.FILTERS.chain().filter(Filters.FILTERS.family().exactMatch(columnFamily))
						.filter(Filters.FILTERS.qualifier().exactMatch("created_date"))
						.filter(Filters.FILTERS.value().range().startClosed(startDate.format(DATE_TIME_FORMATTER))
								.endClosed(endDate.format(DATE_TIME_FORMATTER))))
				.filter(Filters.FILTERS.chain().filter(Filters.FILTERS.family().exactMatch(columnFamily))
						.filter(Filters.FILTERS.qualifier().exactMatch("is_test_subscriber"))
						.filter(Filters.FILTERS.value().exactMatch(isTestSubscriber)));

		// Create a filter list for countries
		Filters.InterleaveFilter interleaveFilter = Filters.FILTERS.interleave();
		for (String country : countriesList) {
			interleaveFilter.filter(Filters.FILTERS.chain().filter(Filters.FILTERS.family().exactMatch(columnFamily))
					.filter(Filters.FILTERS.qualifier().exactMatch("customer_country"))
					.filter(Filters.FILTERS.value().exactMatch(country)));
		}
		chainFilter.filter(interleaveFilter);

		return chainFilter;
	}
 help me with corrections I need to make.
	    
