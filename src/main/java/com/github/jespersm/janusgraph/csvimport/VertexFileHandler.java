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

package com.github.jespersm.janusgraph.csvimport;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.time.StopWatch;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jespersm.janusgraph.csvimport.ColumnHandler.Tag;
import com.github.jespersm.janusgraph.csvimport.schema.SchemaBuilder;
import com.github.jespersm.janusgraph.csvimport.schema.VertexTypeBuilder;
import com.google.common.base.Strings;

public class VertexFileHandler extends AbstractElementFileHandler implements Closeable, AutoCloseable {
	final String vertexLabelName;
	
	private static final Logger LOG = LoggerFactory.getLogger(VertexFileHandler.class);

	private int limitRows;
	public VertexFileHandler(String vertexLabelName, String files, Map<Object, Object> keyMap, int limitRows) throws FileNotFoundException {
		super(files, keyMap);
		this.vertexLabelName = vertexLabelName;
		this.limitRows = limitRows;
	}

	public String getVertexLabelName() {
		return vertexLabelName;
	}
	
	public void insertContent(JanusGraph mainGraph) {
		int verticesCreated = 0;
		StopWatch watch = new StopWatch();
		watch.start();
		Transaction graph = mainGraph.newTransaction();
		try {
			do  {
				if (this.currentParser == null) {
						setupCSVParser(false);
				}
				for (CSVRecord record : currentParser) {
					if (verticesCreated >= limitRows) break;
					
					int columns = Math.min(record.size(), this.columns.length);
					JanusGraphVertex addedVertex = graph.addVertex(vertexLabelName);
					addedVertex.property("_label", vertexLabelName);

					for (int c = 0; c < columns; ++c) {
						ColumnHandler handler = this.columns[c];
						Tag tag = handler.getTag();
						if (tag == ColumnHandler.Tag.IGNORE) continue;
						
						Object value = handler.convert(Strings.emptyToNull(record.get(c)));
						if (value != null) {
							addedVertex.property(handler.getName(), value);
						}
						if (tag == ColumnHandler.Tag.ID) {
							if (keyMap.containsKey(value)) {
								throw new RuntimeException("How did that happen? - Id " + value + " is also defined elsewhere");
							}
							keyMap.put(value, addedVertex.id());
						}
					}
					if (++verticesCreated % 10000 == 0) {
						graph.tx().commit();
						graph.tx().close();
						graph.close();
						graph = mainGraph.newTransaction();
						LOG.info("Created {} {} vertices in {} ms, {} ms/vertex", verticesCreated, vertexLabelName, watch.getTime(), (double) watch.getTime() / verticesCreated); 
					}
				}
				close();
			} while (! files.isEmpty());
			graph.tx().commit();
			graph.tx().close();
			graph.close();
			LOG.info("Created {} {} vertices in {} ms, {} ms/vertex", verticesCreated, vertexLabelName, watch.getTime(), verticesCreated > 0 ? (double) watch.getTime() / verticesCreated : Double.NaN); 
		} catch (IOException e) {
			LOG.error("Error reading file or writing vertex", e);
		}
	}
	public void parseHeaders(SchemaBuilder schemaBuilder) throws IOException {
		setupCSVParser(true);
		int maxColumn = currentParser.getHeaderMap().values().stream().mapToInt(Integer::intValue).max().getAsInt();
		columns = new ColumnHandler[maxColumn+1];
		VertexTypeBuilder vertexBuilder = schemaBuilder.vertex(vertexLabelName);
		
		this.currentParser.getHeaderMap().forEach((label, index) -> {
			ColumnHandler handler = makeColumnHandler(label);
			columns[index] = handler;
			
			// Now create the property
			if (handler.getTag() == ColumnHandler.Tag.ID || handler.getTag() == ColumnHandler.Tag.UNIQUE) {
				vertexBuilder.key(handler.getName(), handler.getDatatype());
			} else if (handler.getTag() == ColumnHandler.Tag.INDEX) {
				vertexBuilder.indexedProperty(handler.getName(), handler.getDatatype());
			} else if (handler.getTag() == ColumnHandler.Tag.DATA) {
				vertexBuilder.property(handler.getName(), handler.getDatatype());
			}
		});
		vertexBuilder.build();
	}
	public String getName() {
		return vertexLabelName;
	}
	
}
