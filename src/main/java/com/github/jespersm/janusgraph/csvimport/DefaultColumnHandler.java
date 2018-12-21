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
