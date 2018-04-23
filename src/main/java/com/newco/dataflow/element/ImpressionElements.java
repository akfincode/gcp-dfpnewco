package com.newco.dataflow.element;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Date;
import java.sql.Timestamp;

public class ImpressionElements implements Serializable { 
	// Must be serializable to be included within a PCollection
	private static final long serialVersionUID = -5502791666748533137L;

	public Integer iMId;
	public Integer iMSK;
	public String iMDateStamp;
	public String iMCurrIndex;
	public Integer iMServedCount;
	public Integer iMTotalImpressions;
	public Integer iMPaidImpressions;
	public String iMActionName;
	public Integer iMFreqCap;
	public Integer iMFillRate;
	public String iMBackfill;
	public String iMBrowser;
	public String iMOSVersion;
	public String dimInsertDate;
	public String dimUpdateDate;
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		try {
			for (Field field: this.getClass().getDeclaredFields()) {
			    if ("serialVersionUID".equals(field.getName())) {
			        continue;
			    }
				if ("symbol".equals(field.getName())) {
					sb.append(field.get(this).toString());
				} else {
					if (null != field.get(this)) {
						sb.append(field.get(this).toString());
					} else {
						sb.append("");
					}
					sb.append("\t");
				}
			}
		} catch (Exception ex) {
			sb.append("<exception reading field: ").append(ex.getMessage()).append(">");
			sb.append("\t");
		}
		return sb.toString();
	}
}
