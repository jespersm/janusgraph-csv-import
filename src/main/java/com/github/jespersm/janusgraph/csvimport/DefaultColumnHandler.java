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

import java.util.function.Function;

final class DefaultColumnHandler<T> implements ColumnHandler<T> {
	private final String fieldPropertyName;
	private final Tag tag;
	private final Class<T> dataType;
	private final Function<String, T> mapper;

	public DefaultColumnHandler(String fieldPropertyName, Tag tag, Class<T> dataType, Function<String, T> mapper) {
		this.fieldPropertyName = fieldPropertyName;
		this.tag = tag;
		this.dataType = dataType;
		this.mapper = mapper;
	}

	public static <R> DefaultColumnHandler<R> of(String fieldPropertyName, Tag tag, Class<R> dataType, Function<String, R> mapper) {
		return new DefaultColumnHandler<R>(fieldPropertyName, tag, dataType, mapper);
	}

	public static DefaultColumnHandler<String> of(String fieldPropertyName, Tag tag) {
		return new DefaultColumnHandler<String>(fieldPropertyName, tag, String.class, Function.identity());
	}

	@Override
	public Class<T> getDatatype() {
		return dataType;
	}

	@Override
	public T convert(String raw) {
		return raw == null ? null : mapper.apply(raw);
	}

	public String getName() {
		return fieldPropertyName;
	}

	@Override
	public Tag getTag() {
		return tag;
	}
}
