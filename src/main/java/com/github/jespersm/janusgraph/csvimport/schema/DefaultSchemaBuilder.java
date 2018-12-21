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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSchemaBuilder implements SchemaBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultSchemaBuilder.class);

	private JanusGraphManagement management;

	public DefaultSchemaBuilder(JanusGraph graph) {
		this.management = graph.openManagement();
	}
	
	@Override
	public VertexTypeBuilder vertex(String labelName) {
		VertexLabel vertexLabel = management.getVertexLabel(labelName);
		LOG.info("{} vertex: " + labelName, vertexLabel == null ? "Creating" : "Verifying");
		return new VertexTypeBuilder() {
			
			List<Consumer<VertexLabel>> doWhenBuilt = new LinkedList<>();
			
			@Override
			public VertexTypeBuilder property(String propertyName, Class<?> type) {
				makeOrVerifyProperty(propertyName, type);
				return this;
			}
			
			@Override
			public VertexTypeBuilder key(String propertyName, Class<?> type) {
				PropertyKey propertyKey = makeOrVerifyProperty(propertyName, type);
				String indexName = "IXU_V_"+ labelName + "_" + propertyName;
				JanusGraphIndex index = management.getGraphIndex(indexName);
				if (index != null) {
					PropertyKey[] keys = index.getFieldKeys();
					LOG.info("Found existing index {}, keys:", indexName, Arrays.asList(keys));
					if (keys.length != 1 || ! keys[0].equals(propertyKey)) {
						throw new RuntimeException(indexName + " doesn't index just " + propertyName);
					}
					if (! index.isUnique()) {
						throw new RuntimeException(indexName + " isn't unique");
					}
 				} else {
					LOG.info("Creating unique index {}", indexName);
					doWhenBuilt.add(newVertex -> 
					management
						.buildIndex(indexName, Vertex.class)
						.indexOnly(newVertex)
						.addKey(propertyKey)
						.unique()
						.buildCompositeIndex());
					
					// TODO: management.addProperties(vertexLabel, keys)
				}
				return this;
			}
			
			@Override
			public VertexTypeBuilder indexedProperty(String propertyName, Class<?> type) {
				PropertyKey propertyKey = makeOrVerifyProperty(propertyName, type);
				String indexName = "IX_V_"+ labelName + "_" + propertyName;
				JanusGraphIndex index = management.getGraphIndex(indexName);
				if (index != null) {
					PropertyKey[] keys = index.getFieldKeys();
					LOG.info("Found existing index {}, keys:", indexName, Arrays.asList(keys));
					if (keys.length != 1 || ! keys[0].equals(propertyKey)) {
						throw new RuntimeException(indexName + " doesn't index just " + propertyName);
					}
					if (index.isUnique()) {
						throw new RuntimeException(indexName + " is unique!");
					}
 				} else {
					LOG.info("Creating non-unique index {}", indexName);
					doWhenBuilt.add(newVertex -> 
					management
						.buildIndex(indexName, Vertex.class)
						.indexOnly(newVertex)
						.addKey(propertyKey)
						.buildCompositeIndex());
				}
				return this;
			}
			
			@Override
			public SchemaBuilder build() {
				if (vertexLabel != null) {
					LOG.info("Verified vertex with label " + labelName);
					doWhenBuilt.forEach(makeConsumer(vertexLabel));
				} else {
					// Nice, let's build it
					LOG.info("Creating a new vertex: " + labelName);
					VertexLabel newLabel = management.makeVertexLabel(labelName).make();
					doWhenBuilt.forEach(makeConsumer(newLabel));
				}
				return DefaultSchemaBuilder.this;
			}

			public VertexTypeBuilder indexOn(String vertexLabelName, String ... propertyNames) {
				VertexLabel vertexLabel = management.getVertexLabel(vertexLabelName);

				String indexName = "IX_V_"+ vertexLabelName + "_" + String.join("#", Arrays.asList(propertyNames));
				for(String pn : propertyNames) {
					indexName += "_" + pn;
				}
				String finalIndexName = indexName;
				
				JanusGraphIndex index = management.getGraphIndex(finalIndexName);
				if (index != null) {
					PropertyKey[] keys = index.getFieldKeys();
					LOG.info("Found existing index {}, keys:", indexName, Arrays.asList(keys));
					if (keys.length != propertyNames.length) {
						throw new RuntimeException(indexName + " wrong number of indexes, expected " + propertyNames.length + " found " + keys.length);
					}
 				} else {
					LOG.info("Creating non-unique index {}", indexName);
					doWhenBuilt.add(newVertex -> { 
						IndexBuilder builder = management
							.buildIndex(finalIndexName, Vertex.class)
							.indexOnly(newVertex);
						for(String pn : propertyNames) {
							builder = builder.addKey(management.getPropertyKey(pn));
						}
						builder.buildCompositeIndex();
					});
 				}
				return this;
			}
		};
	}
	
	public static <T> Consumer<? super Consumer<T>> makeConsumer(T newLabel) {
		return c -> c.accept(newLabel);
	}

	@Override
	public EdgeTypeBuilder edge(String labelName) {
		EdgeLabel edgeLabel = management.getEdgeLabel(labelName);
		LOG.info("{} edge: " + labelName, edgeLabel == null ? "Creating" : "Verifying");
		return new EdgeTypeBuilder() {
			@Override
			public SchemaBuilder build() {
				if (edgeLabel == null) {
					management.makeEdgeLabel(labelName).directed().make();
				}
				return DefaultSchemaBuilder.this;
			}
		};
	}

	public SchemaBuilder property(String propertyName, Class<?> type) {
		makeOrVerifyProperty(propertyName, type);
		return this;
	}

	public PropertyKey makeOrVerifyProperty(String propertyName, Class<?> type) {
		PropertyKey propertyKey = management.getPropertyKey(propertyName);
		if (propertyKey != null) {
			LOG.info("Property already exists: {} - type {}", propertyName, propertyKey.dataType().getSimpleName());
			if (! propertyKey.dataType().equals(type)) {
				throw new RuntimeException(propertyName + ":" + type.getSimpleName() + " mismatches existing datatype " + propertyKey.dataType().getSimpleName());
			}
			if (propertyKey.cardinality() != Cardinality.SINGLE) {
				throw new RuntimeException(propertyName + " isn't SINGLE cadinality");
			}
			return propertyKey;
		} else {
			LOG.info("Creating property {} - type {}", propertyName, type.getSimpleName());
			return management.makePropertyKey(propertyName).cardinality(Cardinality.SINGLE).dataType(type).make();
		}
	}

	/**
	 * Accept the changes and apply them
	 */
	public void done() {
		Objects.requireNonNull(management, "Must not be closed");
		this.management.commit();
		this.management = null;
	}

	@Override
	public void close() {
		if (this.management != null) {
			this.management.rollback();
			management = null;
		}
	}

	@Override
	public SchemaBuilder globalVertexUniqueIndex(String propertyName, Class<?> type) {
		createUniqueIndex(propertyName, type, Vertex.class);
		return this;
	}

	@Override
	public SchemaBuilder globalEdgeUniqueIndex(String propertyName, Class<?> type) {
		createUniqueIndex(propertyName, type, Edge.class);
		return this;
	}

	private void createUniqueIndex(String propertyName, Class<?> type, Class<? extends Element> indexTargetType) {
		PropertyKey propertyKey = makeOrVerifyProperty(propertyName, type);
		String indexName = "IXGU_" + indexTargetType.getSimpleName().substring(0,1) + "_" + propertyName;
		JanusGraphIndex index = management.getGraphIndex(indexName);
		if (index != null) {
			PropertyKey[] keys = index.getFieldKeys();
			LOG.info("Found existing index {}, keys:", indexName, Arrays.asList(keys));
			if (keys.length != 1 || ! keys[0].equals(propertyKey)) {
				throw new RuntimeException(indexName + " doesn't index just " + propertyName);
			}
			if (! index.isUnique()) {
				throw new RuntimeException(indexName + " is not unique!");
			}
			} else {
			LOG.info("Creating non-unique index {}", indexName);
			management
				.buildIndex(indexName, indexTargetType)
				.addKey(propertyKey)
				.unique()
				.buildCompositeIndex();
		}
	}

	@Override
	public SchemaBuilder globalVertexIndex(String propertyName, Class<?> type) {
		createNonUniqueIndex(propertyName, type, Vertex.class);
		return this;
	}

	@Override
	public SchemaBuilder globalEdgeIndex(String propertyName, Class<?> type) {
		createNonUniqueIndex(propertyName, type, Edge.class);
		return this;
	}
	
	private void createNonUniqueIndex(String propertyName, Class<?> type, Class<? extends Element> indexTargetType) {
		PropertyKey propertyKey = makeOrVerifyProperty(propertyName, type);
		String indexName = "IXG_" + indexTargetType.getSimpleName().substring(0,1) + "_" + propertyName;
		JanusGraphIndex index = management.getGraphIndex(indexName);
		if (index != null) {
			PropertyKey[] keys = index.getFieldKeys();
			LOG.info("Found existing index {}, keys:", indexName, Arrays.asList(keys));
			if (keys.length != 1 || ! keys[0].equals(propertyKey)) {
				throw new RuntimeException(indexName + " doesn't index just " + propertyName);
			}
			if (index.isUnique()) {
				throw new RuntimeException(indexName + " is unique!");
			}
			} else {
			LOG.info("Creating non-unique index {}", indexName);
			management
				.buildIndex(indexName, Vertex.class)
				.addKey(propertyKey)
				.buildCompositeIndex();
		}
	}

}
