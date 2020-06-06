package de.kp.works.graph;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;

@Plugin(type = "batchsink")
@Name("GraphSink")
@Description("A batch sink to write structured records to a distributed in-memory graph database.")
public class GraphSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {

	private GraphConfig config;
	
	public GraphSink(GraphConfig config) {
		this.config = config;
	}
	
	 @Override
	  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
	    super.configurePipeline(pipelineConfigurer);

	    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
	    config.validateSchema(inputSchema);
	    
	    config.validateConnection();

	 }
	
	/*
	 * The emitter is based on a key-value tuple (NullWritable, StructuredRecord)
	 * where output information (to graph database) is restricted to the value part
	 */
	@Override
	public void prepareRun(BatchSinkContext context) throws Exception {
		
		// TODO
		Schema inputSchema = context.getInputSchema();
		context.addOutput(Output.of(config.referenceName, new GraphOutputFormatProvider(config, inputSchema)));

	}

	@Override
	public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, StructuredRecord>> emitter)
			throws Exception {
		/*
		 * This approach sends a sequence of structured records, each record
		 * individually to the Graph database backend
		 */
		emitter.emit(new KeyValue<NullWritable, StructuredRecord>(null, input));
	}
	
	private static class GraphOutputFormatProvider implements OutputFormatProvider {

		private final Map<String, String> conf;

		GraphOutputFormatProvider(GraphConfig graphConfig, Schema schema) {

			this.conf = new HashMap<>();
			
			// TODO

		}

		/**
		 * The following method connect the output format provider with a specific
		 * output format and the dynamically delivered configuration
		 */
		@Override
		public String getOutputFormatClassName() {
			return GraphOutputFormat.class.getName();
		}

		@Override
		public Map<String, String> getOutputFormatConfiguration() {
			return conf;
		}

	}

}
