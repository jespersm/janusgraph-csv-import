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
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jespersm.janusgraph.csvimport.schema.DefaultSchemaBuilder;
import com.github.jespersm.janusgraph.csvimport.utils.IOConsumer;

import picocli.CommandLine;
import picocli.CommandLine.DefaultExceptionHandler;
import picocli.CommandLine.Help;
import picocli.CommandLine.Option;
import picocli.CommandLine.RunLast;

public class Import implements Callable<Void> {
	private static final Logger LOG = LoggerFactory.getLogger(Import.class);

/*	static enum Mode {
		CSV, TSV;
	}
	
	@Option(names = {"--mode"}, description = "Valid values: ${COMPLETION-CANDIDATES}")
    private Mode mode;
*/	
	/*
	 * @Option(names = {"--multiline-fields"})
	 * private boolean multilineFields = false;
	 */
	
	@Option(names = {"--ignore-missing-nodes"})
    private boolean ignoreMissingNodes = true;
	
	/*
	@Option(names = {"--ignore-errors"})
    private boolean ignoreErrors = false;
	*/
	
	@Option(names = {"--add-label-property"})
    private boolean addLabelProperty = true;
	
	@Option(names = {"-c", "--config"}, required=true)
    private String configFile;

//    @Option(names = {"--id-type"})
//    private String idType;

	@Option(names = {"-n", "--limit-rows"})
    private int limitRows = -1;
	
    @Option(names = {"-D", "--drop-before-import"})
    private boolean drop = false;

    @Option(names = {"--threads"}, description = "Number of threads to run concurrently")
    private int poolSize = 2;
    
    @Option(names = {"-i", "--index"}, split=",")
    private Set<String> index = new LinkedHashSet<>();

    @Option(names = {"-g", "--global-index"}, split=",")
    private Set<String> globalIndex = new LinkedHashSet<>();

    @Option(names = {"--edgeLabels"}, split=",")
    private List<String> edgeLabels = new LinkedList<>();

    @Option(names = {"--nodes"}, required=true)
    private Map<String, String> nodes = new LinkedHashMap<>();
    
    @Option(names = {"--relationships"})
    private List<String> relationships = new LinkedList<>();
    
	@Override
	public Void call() throws Exception {

		if (limitRows < 0) limitRows = Integer.MAX_VALUE-1;
		
		ConcurrentHashMap<Object, Object> keyMap = new ConcurrentHashMap<Object, Object>();
		List<VertexFileHandler> vertexHandlers = new LinkedList<>();
		List<EdgeFileHandler> edgeHandlers = new LinkedList<>();
		
		try {
			// Open the vertex headers
			for (Map.Entry<String, String> entry : nodes.entrySet()) {
				VertexFileHandler handler = new VertexFileHandler(entry.getKey(), entry.getValue(), keyMap, limitRows);
				vertexHandlers.add(handler);
			}

			// Open the edge headers
			for (String files : relationships) {
				EdgeFileHandler handler = new EdgeFileHandler(files, keyMap, limitRows, ignoreMissingNodes);
				edgeHandlers.add(handler);
			}

			try(JanusGraph graph = initializeGraph()) {
		
				LOG.info("*** Building schema:");
				try(DefaultSchemaBuilder schema = new DefaultSchemaBuilder(graph)) {
					forEach(vertexHandlers, handler -> handler.parseHeaders(schema));
					forEach(edgeHandlers, handler -> handler.parseHeaders(schema));
					
					forEach(edgeLabels, label -> schema.edge(label.trim()).build());
					schema.globalVertexIndex("_label", String.class);
					schema.done();
				}				
				
				LOG.info("*** Creating vertices:");
				doWithExecutor(executor -> {
					forEach(vertexHandlers, (h) -> {
						executor.execute(() -> {
							LOG.info("Starting to write vertices: {}", h.getVertexLabelName());
							try {
								h.insertContent(graph);
								LOG.info("Done writing vertices: {}", h.getVertexLabelName());
							} catch (Exception e) {
								LOG.error("Error handling vertex label: " + h.getVertexLabelName(), e);
							}
						});
					});
				});
				
				LOG.info("*** Creating edge:");
				doWithExecutor(executor -> {
					forEach(edgeHandlers, (h) -> {
						executor.execute(() -> {
							String desc = h.getDescription();
							LOG.info("Starting to write edges from: {}", desc);
							try {
								h.insertContent(graph);
								LOG.info("Starting to write edges from: {}", desc);
							} catch (Exception e) {
								LOG.error("Error handling vertex label: " + desc, e);
							}
						});
					});
				});
			}
		} finally {
			LOG.info("Closing handlers");
			forEach(vertexHandlers, Closeable::close);
			forEach(edgeHandlers, Closeable::close);
		}
		LOG.info("Done importing");
        return null;
	}

	private JanusGraph initializeGraph() throws BackendException {
		LOG.info("Opening graph from information in {}", configFile);
		JanusGraph graph = JanusGraphFactory.open(configFile);
		if (drop) {
			LOG.info("DROPPING GRAPH AT {}!", configFile);
			JanusGraphFactory.drop(graph);
			graph = JanusGraphFactory.open(configFile);
		}
		return graph;
	}

	private <T> void forEach(Iterable<T> handlers, IOConsumer<T> consumer) throws IOException {
		for (T vfe : handlers) {
			consumer.accept(vfe);
		}
	}

	void doWithExecutor(IOConsumer<ExecutorService> consumer) throws IOException {
		ExecutorService executor = Executors.newFixedThreadPool(poolSize);
		consumer.accept(executor);
		LOG.info("Awaiting termination of jobs");
		awaitTerminationAfterShutdown(executor);
	}	
	
	public static void awaitTerminationAfterShutdown(ExecutorService threadPool) {
	    threadPool.shutdown();
	    try {
	        if (!threadPool.awaitTermination(48, TimeUnit.HOURS)) {
	            threadPool.shutdownNow();
	        }
	    } catch (InterruptedException ex) {
	        threadPool.shutdownNow();
	        // We've been asked to shut down, maybe Ctrl+C
	        try {
				threadPool.awaitTermination(120, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
		        Thread.currentThread().interrupt();
		        return;
			}
	        Thread.currentThread().interrupt();
	    }
	}
	
	public static void main(String[] args) {
		new CommandLine(new Import())
			.setCaseInsensitiveEnumValuesAllowed(true)
			.parseWithHandlers(
				new RunLast()
					.useOut(System.out)
					.useAnsi(Help.Ansi.AUTO),
					new DefaultExceptionHandler<List<Object>>()
						.useErr(System.err)
						.useAnsi(Help.Ansi.AUTO),
				args);
	}
}	

/*
	PropertyKey flavourKey = management.makePropertyKey("flavour").dataType(String.class).make();
	PropertyKey weightKey = management.makePropertyKey("weight").dataType(Double.class).make();
	PropertyKey styleKey = management.makePropertyKey("style").dataType(String.class).make();
	VertexLabel pieLabel = management.makeVertexLabel("Pie").make();
	VertexLabel breadLabel = management.makeVertexLabel("Bread").make();
	VertexLabel dummyHealthCheckLabel = management.makeVertexLabel("DummyHealthCheck").make();
	
	management.buildIndex("my_ix", Vertex.class).indexOnly(pieLabel).addKey(flavourKey).buildCompositeIndex();
	management.commit();

	graph.addVertex(T.label, "Pie", "flavour", "apple", "weight", 623.2);
	graph.addVertex(T.label, "Pie", "flavour", "apple", "weight", 754.73);
	graph.addVertex(T.label, "Bread", "style", "rye", "weight", 953.1);
	graph.tx().commit();
	
	LOG.info("Now traversing for Pies");
	graph.traversal().V().hasLabel("Pie").has("flavour", "apple").toList().forEach(
			v -> System.out.println(v + " - " + v.value("weight") +  " grams"));
	graph.traversal().V().label().groupCount().forEachRemaining(m -> 
		m.forEach((k, v) -> System.out.println("Label: " + k + " count: " + v)));
	LOG.info("Done with pies");
	System.out.println("Count of all vertices: " + graph.traversal().V().count().next()); 
	LOG.info("Done with silly questions");
	graph.close();
*/

	