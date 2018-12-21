// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.github.jespersm.janusgraph.csvimport.schema;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphOutput implements AutoCloseable, Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(GraphOutput.class);

	public JanusGraph graph = null;
	
	public void withSchema(Consumer<SchemaBuilder> builder) {
		Objects.requireNonNull(graph, "Graph must be open");
		try(DefaultSchemaBuilder schemaVerifier = new DefaultSchemaBuilder(graph)) {
			builder.accept(schemaVerifier);
			schemaVerifier.done();
		}
	}
	
	public void open() throws InterruptedException, ExecutionException, TimeoutException {
		this.graph = JanusGraphFactory.open("src/main/resources/janusgraph-import.properties");
		
	}
	
	public JanusGraph getGraph() {
		return graph;
	}
	
	@Override
	public void close() {
		if (graph != null) {
			graph.close();
		}
	}

	public void addVertex(String label, Object ... propertyKeyValues) {
		JanusGraphVertex vertex = graph.addVertex(label);
	    for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
	        if (propertyKeyValues[i+1] != null)
	            vertex.property((String) propertyKeyValues[i], propertyKeyValues[i + 1]);
	    }
	}
}
