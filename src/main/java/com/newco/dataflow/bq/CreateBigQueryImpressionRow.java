package com.newco.dataflow.bq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.newco.dataflow.transform.ExtractLineItemElements;
import com.newco.dataflow.element.ImpressionElements;

/**
 * {@link DoFn} Contains logic for transforming data into BigQuery
 * <p>
 * Class definition extends DoFn, which is represented to take in a String and return a String.
 *
 */
public class CreateBigQueryImpressionRow extends DoFn < ImpressionElements, TableRow > {
	private static final long serialVersionUID = 793525232201375933L;
	private static final Logger LOGGER = LoggerFactory.getLogger(CreateBigQueryImpressionRow.class);
	private final Aggregator < Integer, Integer > processed;
	private final Aggregator < Integer, Integer > exceptions;

	public static final TableRow toBigQuery(ImpressionElements element) {
		TableRow row = new TableRow();

		row.set("IM_Id", element.iMId);
		row.set("IM_SK", element.iMSK);
		row.set("IM_DateStamp", element.iMDateStamp);
		row.set("Def_Curr_Row_Index", element.iMCurrIndex);
		row.set("IM_Total_Code_Served_Count", element.iMServedCount);
		row.set("IM_Total_Impressions", element.iMTotalImpressions);
		row.set("IM_Total_Paid_Impressions", element.iMPaidImpressions);
		row.set("IM_ActionName", element.iMActionName);
		row.set("IM_FrequencyCap", element.iMFreqCap);
		row.set("IM_Fill_Rate", element.iMFillRate);
		row.set("IM_Backfill", element.iMBackfill);
		row.set("IM_Browser", element.iMBrowser);
		row.set("IM_OSVersion", element.iMOSVersion);
		row.set("Dim_Insert_Date", element.dimInsertDate);
		row.set("Dim_Update_Date", element.dimUpdateDate);

		return row;
	}

    public CreateBigQueryImpressionRow() {
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
