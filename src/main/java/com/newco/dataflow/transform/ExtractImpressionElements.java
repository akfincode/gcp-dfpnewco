package com.newco.dataflow.transform;

import java.math.BigDecimal;
import java.util.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.newco.dataflow.element.ImpressionElements;

/**
 * {@link DoFn} Contains the logic for transforming line items data into BigQuery
 * <p>
 * Class definition extends DoFn, which is represented to take in a String and return a String.
 */
public class ExtractImpressionElements extends DoFn < String, ImpressionElements > {
	private static final long serialVersionUID = 8935252322013725933L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ExtractImpressionElements.class);
	private final Aggregator < Integer, Integer > processed;
	private final Aggregator < Integer, Integer > exceptions;
	//private DateFormat df;
	
	public ExtractImpressionElements() {
		this.processed = createAggregator("Processed symbols", new Sum.SumIntegerFn());
		this.exceptions = createAggregator("Exceptions", new Sum.SumIntegerFn());
	}

	/**
	 * Extract the elements.
	 * <p>
	 */
	public void processElement(ProcessContext c) {
		try {
			//df = new SimpleDateFormat("YYYY-MM-DD"); 
			
			String line = c.element();			
			String[] values = line.split("\t");

			ImpressionElements element = new ImpressionElements();
			
			if (values[0] != null) {
				if (values[0].length() > 0) {
					element.iMId = new Integer(values[0]);
				}
			}			
			if (values[1] != null) {
				if (values[1].length() > 0) {
					element.iMSK = new Integer(values[1]);
				}
			}
			if (values[2] != null) {
				if (values[2].length() > 0) {
					element.iMDateStamp = values[2];
				}
			}
			if (values[3] != null) {
				if (values[3].length() > 0) {
					element.iMCurrIndex = values[3];
				}
			}
			if (values[4] != null) {
				if (values[4].length() > 0) {
					element.iMServedCount = new Integer(values[4]);
				}
			}
			if (values[5] != null) {
				if (values[5].length() > 0) {
					element.iMTotalImpressions = new Integer(values[5]);
				}
			}			
			if (values[6] != null) {
				if (values[6].length() > 0) {
					element.iMPaidImpressions = new Integer(values[5]);
				}
			}			
			if (values[7] != null) {
				if (values[7].length() > 0) {
					element.iMActionName = values[7];
				}
			}
			if (values[8] != null) {
				if (values[8].length() > 0) {
					element.iMFreqCap = new Integer(values[8]);
				}
			}
			if (values[9] != null) {
				if (values[9].length() > 0) {
					element.iMFillRate = new Integer(values[9]);
				}
			}
			if (values[10] != null) {
				if (values[10].length() > 0) {
					element.iMBackfill = values[10];
				}
			}
			if (values[11] != null) {
				if (values[11].length() > 0) {
					element.iMBrowser = values[11];
				}
			}
			if (values[12] != null) {
				if (values[12].length() > 0) {
					element.iMOSVersion = values[12];
				}
			}
			if (values[13] != null) {
				if (values[13].length() > 0) {
					element.dimInsertDate = values[13];
				}
			}		
			if (values[14] != null) {
				if (values[14].length() > 0) {
					element.dimUpdateDate =values[14];
				}
			}	
			
			c.output(element); // adds to the output PCollection
			processed.addValue(1); // increment aggregator
			return;

		} catch (Exception ex) {
			// Aggregate exception counts for at-a-glance monitoring via Dataflow console
			exceptions.addValue(1);
			LOGGER.warn(ex.getMessage(), ex);
			return;
		}
	}
}
