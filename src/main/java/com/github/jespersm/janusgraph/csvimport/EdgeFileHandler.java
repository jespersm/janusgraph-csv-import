package com.github.jespersm.janusgraph.csvimport;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jespersm.janusgraph.csvimport.ColumnHandler.Tag;
import com.github.jespersm.janusgraph.csvimport.schema.SchemaBuilder;
import com.google.common.base.Strings;

public class EdgeFileHandler extends AbstractElementFileHandler implements Closeable, AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(EdgeFileHandler.class);
	private int limitRows;
	private boolean ignoreMissingNodes;
	
	public EdgeFileHandler(String files, Map<Object, Object> keyMap, int limitRows, boolean ignoreMissingNodes) throws FileNotFoundException {
		super(files, keyMap);
		this.limitRows = limitRows;
		this.ignoreMissingNodes = ignoreMissingNodes;
	}

	public void insertContent(JanusGraph mainGraph) {
		int edgesCreated = 0;
		int startColumn = findTag(ColumnHandler.Tag.START_ID);
		int endColumn = findTag(ColumnHandler.Tag.END_ID);
		int typeColumn = findTag(ColumnHandler.Tag.TYPE);

		if (startColumn == -1) {
			throw new RuntimeException("No start-column for relationship");
		}
		if (endColumn == -1) {
			throw new RuntimeException("No end-column for relationship");
		}
		if (typeColumn == -1) {
			throw new RuntimeException("No type column for relationship");
		}
		ColumnHandler startHandler = columns[startColumn];
		ColumnHandler endHandler = columns[endColumn];
		ColumnHandler typeHandler = columns[typeColumn];
		
		StopWatch watch = new StopWatch();
		watch.start();
		Transaction graph = mainGraph.newTransaction();
		try {
			do  {
				if (this.currentParser == null) {
						setupCSVParser(false);
				}
				for (CSVRecord record : currentParser) {
					if (edgesCreated >= limitRows) break;
					int columns = Math.min(record.size(), this.columns.length);
					
					Object startId = startHandler.convert(record.get(startColumn));
					if (startId == null) {
						LOG.debug("Start-id field of edge record #{} of {} missing", currentParser.getRecordNumber(), currentFile);
						continue;
					}
					Object endId = endHandler.convert(record.get(endColumn));
					if (endId == null) {
						LOG.debug("End-id field of edge record #{} of {} missing -- skipping", endId, currentFile);
						continue;
					}

					Object janusStartKey = keyMap.get(startId);
					Object janusEndKey = keyMap.get(endId);
					
					if (janusStartKey == null) {
						LOG.debug("Making Edge from {}, but vertex wasn't created", startId);
						if (! ignoreMissingNodes) {
							LOG.error("Making Edge from {}, but vertex wasn't created -- aborting", startId);
							break;
						}
						continue;
					}
					if (janusEndKey == null) {
						LOG.debug("Making Edge to {}, but vertex wasn't created", endId);
						if (! ignoreMissingNodes) {
							LOG.error("Making Edge to {}, but vertex wasn't created -- aborting", endId);
							break;
						}
						continue;
					}
					Iterator<Vertex> vertices = graph.vertices(janusStartKey, janusEndKey);
					if (! vertices.hasNext()) {
						LOG.warn("Vertex with id {} (graph id {}) couldn't be found -- skipping", startId, janusStartKey);
						continue;
					}
					Vertex fromVertex = vertices.next();
					if (! vertices.hasNext()) {
						LOG.warn("Vertex with id {} (graph id {}) couldn't be found -- skipping", endId, janusEndKey);
						continue;
					}
					Vertex toVertex = vertices.next();
					String typeName = (String)typeHandler.convert(record.get(typeColumn));
					Edge addedEdge = fromVertex.addEdge(typeName, toVertex);
				
					addedEdge.property("_label", typeName);
					
					for (int c = 0; c < columns; ++c) {
						ColumnHandler handler = this.columns[c];
						Tag tag = handler.getTag();
						if (tag == ColumnHandler.Tag.IGNORE || tag == ColumnHandler.Tag.START_ID || tag == ColumnHandler.Tag.END_ID || tag == ColumnHandler.Tag.TYPE) continue;
						
						Object value = handler.convert(Strings.emptyToNull(record.get(c)));
						if (value != null) {
							addedEdge.property(handler.getName(), value);
						}
					}
					if (++edgesCreated % 10000 == 0) {
						graph.tx().commit();
						graph.tx().close();
						graph.close();
						graph = mainGraph.newTransaction();
						LOG.info("Created {} edges in {} ms, {} ms/edge", edgesCreated, watch.getTime(), (double) watch.getTime() / edgesCreated); 
					}
				}
				close();
			} while (! files.isEmpty());
			graph.tx().commit();
			graph.tx().close();
			graph.close();
			
			LOG.info("Created {} edges in {} ms, {} ms/edge", edgesCreated, watch.getTime(), (edgesCreated > 0 ? ((double) watch.getTime() / edgesCreated) : Double.NaN)); 
		} catch (IOException e) {
			LOG.error("Error reading file or writing vertex", e);
		}
	}
	
	private int findTag(Tag tag) {
		for (int i = 0 ; i < columns.length; ++i) {
			if (columns[i].getTag() == tag) return i;
		}
		return -1;
	}

	public void parseHeaders(SchemaBuilder schemaBuilder) throws IOException {
		setupCSVParser(true);
		int maxColumn = currentParser.getHeaderMap().values().stream().mapToInt(Integer::intValue).max().getAsInt();
		columns = new ColumnHandler[maxColumn+1];
		
		this.currentParser.getHeaderMap().forEach((label, index) -> {
			ColumnHandler handler = makeColumnHandler(label);
			columns[index] = handler;
			
			Tag tag = handler.getTag();
			if (! handler.getName().equals("")
					&& tag != ColumnHandler.Tag.END_ID
					&& tag != ColumnHandler.Tag.START_ID
				    && tag != ColumnHandler.Tag.TYPE
				    && tag != ColumnHandler.Tag.IGNORE) {
				schemaBuilder.property(handler.getName(), handler.getDatatype());
			}
		});
	}
	
	public String getDescription() {
		return String.join(", ", files.stream().map(Object::toString).collect(Collectors.toList()));
	}
	
}
