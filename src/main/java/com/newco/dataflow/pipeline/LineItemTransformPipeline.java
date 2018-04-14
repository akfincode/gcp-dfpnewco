package com.newco.dataflow.pipeline;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.newco.dataflow.bq.CreateBigQueryImpressionRow;
import com.newco.dataflow.bq.CreateBigQueryLineItemRow;
import com.newco.dataflow.element.ImpressionElements;
import com.newco.dataflow.element.LineItemElements;
import com.google.cloud.dataflow.sdk.options.ValueProvider;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.newco.dataflow.transform.ExtractImpressionElements;
import com.newco.dataflow.transform.ExtractLineItemElements;

public class LineItemTransformPipeline {
	private static final Logger LOGGER = LoggerFactory.getLogger(LineItemTransformPipeline.class);

	/**
	 * Options supported by {@link LineItemTransformPipeline}.
	 * <p>Custom representation of options required by this particular pipeline class.
	 * Any property not covered by the base PipelineOptions class should be added to a subclass.
	 * This will take advantage of the mapping of any CLI parameters to Java class properties.</p>
	 */
	public static interface LineItemTransformOptions extends PipelineOptions {

		@Description("Path to input file")@Default.String("")
		ValueProvider<String> getInputFilePath();
		void setInputFilePath(ValueProvider<String> value);

		@Description("Output file path")@Default.String("")
		ValueProvider<String> getOutputFilePath();
		void setOutputFilePath(ValueProvider<String> value);

		@Description("Output table")@Default.String("")
		String getOutputTable();
		void setOutputTable(String value);
	}

	/**
	 * {@link Pipeline} Entry point of this Dataflow Pipeline
	 */
	public static void main(String args[]) {

		try {

			ExtractLineItemElements fn = new ExtractLineItemElements();
			ExtractImpressionElements fnIM = new ExtractImpressionElements();
			
			LineItemTransformOptions options = PipelineOptionsFactory.fromArgs(args).as(LineItemTransformOptions.class);
						
			Pipeline pipeline = Pipeline.create(options);

			// Input is a PCollection of String, Output is a PCollection of LineItemElements
			PCollection < LineItemElements > mainCollection = pipeline.apply(TextIO.Read.named("Reading DFP NewCo Input File")
				.from(options.getInputFilePath()).withoutValidation())
				.apply(ParDo.named("Extracting LineItem")
				.of(fn)).setCoder(SerializableCoder.of(LineItemElements.class));
							
			// Destination is BigQuery - DataflowPipelineRunner will be used
			if (!"".equals(options.getOutputTable())) {
				mainCollection.apply(ParDo.named("Creating LineItem BigQuery Row")
					.of(new CreateBigQueryLineItemRow()))
					.apply(BigQueryIO.Write.named("Writing LineItem Records to BigQuery")
					.to(options.getOutputTable())
					.withSchema(LineItemTransformPipeline.generateLineItemSchema())
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
			} 

			// IMPRESSIONS
			// Input is a PCollection of String, Output is a PCollection of Impressions
			PCollection < ImpressionElements > mainCollectionIM = pipeline.apply(TextIO.Read.named("Reading DFP NewCo Impressions Input File")
				.from("gs://phrasal-matrix-8788/input/impression.csv").withoutValidation())
				.apply(ParDo.named("Extracting Impressions")
				.of(fnIM)).setCoder(SerializableCoder.of(ImpressionElements.class));
							
			// Destination is BigQuery - DataflowPipelineRunner will be used
			if (!"".equals(options.getOutputTable())) {
				mainCollectionIM.apply(ParDo.named("Creating Impression BigQuery Row")
					.of(new CreateBigQueryImpressionRow()))
					.apply(BigQueryIO.Write.named("Writing Impression Records to BigQuery")
					.to("braided-tracker-159814:DFPAllClients.Impression")
					.withSchema(LineItemTransformPipeline.generateImpressionSchema())
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
			} 

			pipeline.run();
			System.exit(0);

		} catch (Exception ex) {
			LOGGER.error(ex.getMessage(), ex);
			System.exit(1);
		}
	}
	
	public static TableSchema generateLineItemSchema() {
		List < TableFieldSchema > fields = new ArrayList < TableFieldSchema > ();
	
		fields.add(new TableFieldSchema().setName("LI_Id").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("LI_SK").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("CL_Name").setType("STRING"));
		fields.add(new TableFieldSchema().setName("IM_Id").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("LI_DateStamp").setType("STRING"));
		fields.add(new TableFieldSchema().setName("LI_CPM").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("LI_Type").setType("STRING"));
		fields.add(new TableFieldSchema().setName("LI_Publisher_Impressions").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("LI_Publisher_Coverage").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("LI_Pub_Monitized_PageViews").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("LI_Impressions_per_Session").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("LI_Publisher_Clicks").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("LI_Publisher_CTR").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("LI_Publisher_Revenue").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("LI_Publisher_eCPM").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("LI_Frequency_Cap").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("LI_Fill").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("LI_Region").setType("STRING"));
		fields.add(new TableFieldSchema().setName("LI_Source").setType("STRING"));		
		fields.add(new TableFieldSchema().setName("Dim_Insert_Date").setType("STRING"));
		fields.add(new TableFieldSchema().setName("Dim_Update_Date").setType("STRING"));
		
		return new TableSchema().setFields(fields);
	}
	
	public static TableSchema generateImpressionSchema() {
		List < TableFieldSchema > fields = new ArrayList < TableFieldSchema > ();
		
		fields.add(new TableFieldSchema().setName("IM_Id").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("IM_SK").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("IM_DateStamp").setType("STRING"));
		fields.add(new TableFieldSchema().setName("Def_Curr_Row_Index").setType("STRING"));
		fields.add(new TableFieldSchema().setName("IM_Total_Code_Served_Count").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("IM_Total_Impressions").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("IM_Total_Paid_Impressions").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("IM_ActionName").setType("STRING"));
		fields.add(new TableFieldSchema().setName("IM_FrequencyCap").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("IM_Fill_Rate").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("IM_Backfill").setType("STRING"));
		fields.add(new TableFieldSchema().setName("IM_Browser").setType("STRING"));
		fields.add(new TableFieldSchema().setName("IM_OSVersion").setType("STRING"));
		fields.add(new TableFieldSchema().setName("Dim_Insert_Date").setType("STRING"));
		fields.add(new TableFieldSchema().setName("Dim_Update_Date").setType("STRING"));
		
		return new TableSchema().setFields(fields);
	}
}