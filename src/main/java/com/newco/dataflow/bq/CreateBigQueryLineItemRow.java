package com.newco.dataflow.bq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.newco.dataflow.transform.ExtractLineItemElements;
import com.newco.dataflow.element.LineItemElements;

/**
 * {@link DoFn} Contains logic for transforming data into BigQuery
 * <p>
 * Class definition extends DoFn, which is represented to take in a String and return a String.
 */
public class CreateBigQueryLineItemRow extends DoFn < LineItemElements, TableRow > {
	private static final long serialVersionUID = 7935252322013725933L;
	private static final Logger LOGGER = LoggerFactory.getLogger(CreateBigQueryLineItemRow.class);
	private final Aggregator < Integer, Integer > processed;
	private final Aggregator < Integer, Integer > exceptions;

	public static final TableRow toBigQuery(LineItemElements element) {
		TableRow row = new TableRow();

		row.set("LI_Id", element.lIId);
		row.set("LI_SK", element.lISK);
		row.set("CL_Name", element.cLName);
		row.set("IM_Id", element.iMId); 
		row.set("LI_DateStamp", element.lIDateStamp);
		row.set("LI_CPM", element.lICPM);
		row.set("LI_Publisher_Impressions", element.lIPublisherImpressions);
		row.set("LI_Publisher_Coverage", element.lIPublisherCoverage);
		row.set("LI_Pub_Monitized_PageViews", element.lIPubMonitizedPageViews);
		row.set("LI_Impressions_per_Session", element.lIImpressionsSession);
		row.set("LI_Publisher_Clicks", element.lIPublisherClicks);
		row.set("LI_Publisher_CTR", element.lIPublisherCTR);
		row.set("LI_Frequency_Cap", element.lIFrequencyCap);
		row.set("LI_Fill", element.lIFill);
		row.set("LI_Region", element.lIRegion);
		row.set("LI_Source", element.lISource);
		row.set("LI_Publisher_Revenue", element.lIPublisherRevenue);
		row.set("LI_Type", element.lIType);
		row.set("LI_Publisher_eCPM", element.lIPublishereCPM);
		row.set("Dim_Insert_Date", element.dimInsertDate);
		row.set("Dim_Update_Date", element.dimUpdateDate);

		return row;
	}

    public CreateBigQueryLineItemRow() {
        this.processed = createAggregator("Processed items", new Sum.SumIntegerFn());
        this.exceptions = createAggregator("Exceptions", new Sum.SumIntegerFn());
    }

	/**
	 * Take a LineItem and return a BigQuery TableRow
	 */
	public void processElement(ProcessContext c) {
		try {
			c.output(toBigQuery(c.element()));
			processed.addValue(1); // increment aggregator
			return;

		} catch (Exception ex) {
			exceptions.addValue(1);
			LOGGER.warn(ex.getMessage(), ex);
		}
	}
}
