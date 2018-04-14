package com.newco.dataflow.element;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Date;
import java.sql.Timestamp;

public class LineItemElements implements Serializable { 
	// Must be serializable to be included within a PCollection
    private static final long serialVersionUID = -4502791666748533137L;
  
    public Integer lIId;
    public Integer lISK;
    public String cLName;
    public Integer iMId;
    public String lIDateStamp;
	public BigDecimal lICPM;
	public String lIType;
	public Integer lIPublisherImpressions;
	public Integer lIPublisherCoverage;
	public Integer lIPubMonitizedPageViews;
	public Integer lIImpressionsSession;
	public Integer lIPublisherClicks;
	public Integer lIPublisherCTR;
	public BigDecimal lIPublisherRevenue;
	public BigDecimal lIPublishereCPM;	
	public Integer lIFrequencyCap;
	public Integer lIFill;	
	public String lIRegion;
	public String lISource;
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
