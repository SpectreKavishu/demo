package com.dnb.vbo.pricing.database.model;
import java.time.LocalDate;
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
