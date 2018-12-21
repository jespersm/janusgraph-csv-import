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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

public class AbstractElementFileHandler {

	protected Map<Object, Object> keyMap;
	protected final Deque<File> files = new LinkedList<File>();
	protected ColumnHandler[] columns = null;
	protected CSVParser currentParser = null;
	protected File currentFile = null;

	public AbstractElementFileHandler(String files, Map<Object, Object> keyMap) throws FileNotFoundException {
		this.keyMap = keyMap;
		for (String s : files.split(",")) {
			File f = new File(s);
			if (! f.exists() || ! f.isFile()) {
				throw new FileNotFoundException("File " + s + " not found");
			}
			this.files.add(f);
		}
	}

	protected ColumnHandler<?> makeColumnHandler(String header) {
		
		String[] parts = header.split(":");
		String fieldPropertyName = parts.length > 0 ? parts[0] : "";

		String typeName = "string";
		ColumnHandler.Tag tag = ColumnHandler.Tag.DATA;
		
		if (parts.length >=3) {
			typeName = parts[1];
			tag = ColumnHandler.Tag.valueOf(parts[2]);
		} else if (parts.length == 2) {
			try {
				tag = ColumnHandler.Tag.valueOf(parts[1]);
			} catch (Exception e) {
				typeName = parts[1];
			}
		}
		if (fieldPropertyName.equals("uuid")) {
			typeName = "uuid";
		}
		switch (typeName) {
		case "int":
			return DefaultColumnHandler.of(fieldPropertyName, tag, Integer.class, Integer::valueOf);
		case "long":
			return DefaultColumnHandler.of(fieldPropertyName, tag, Long.class, Long::valueOf);
		case "float":
			return DefaultColumnHandler.of(fieldPropertyName, tag, Float.class, Float::valueOf);
		case "double":
			return DefaultColumnHandler.of(fieldPropertyName, tag, Double.class, Double::valueOf);
		case "boolean":
			return DefaultColumnHandler.of(fieldPropertyName, tag, Boolean.class, Boolean::valueOf);
		case "byte":
			return DefaultColumnHandler.of(fieldPropertyName, tag, Byte.class, Byte::valueOf);
		case "short":
			return DefaultColumnHandler.of(fieldPropertyName, tag, Short.class, Short::valueOf);
		case "char":
			return DefaultColumnHandler.of(fieldPropertyName, tag, Character.class, s -> s.charAt(0));
		case "datetime":
			return DefaultColumnHandler.of(fieldPropertyName, tag, Date.class, s -> Date.from(ZonedDateTime.parse(s).toInstant()));
		case "uuid":
			return DefaultColumnHandler.of(fieldPropertyName, tag, UUID.class, UUID::fromString);
		case "date":
		case "localtime":
		case "time":
		case "localdatetime":
		case "duration":
		default:
			return DefaultColumnHandler.of(fieldPropertyName, tag);
		}
	}

	public void setupCSVParser(boolean isFirst) throws IOException {
		if (this.currentParser != null) {
			this.currentParser.close();
			this.currentParser = null;
		}
		File file = files.removeFirst();
		CSVFormat format = isFirst ? CSVFormat.DEFAULT.withFirstRecordAsHeader() : CSVFormat.DEFAULT;
		BufferedReader mappingReader = null;
	    CSVParser mappingParser = null;
		try {
			mappingReader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8);
	        mappingParser = new CSVParser(mappingReader, format);
			// Transfer ownership
			this.currentParser = mappingParser;
			this.currentFile = file;
			mappingParser = null;
			mappingReader = null;
		}  finally {
			if (mappingParser != null) {
				mappingParser.close();
			} else if (mappingReader != null) {
				mappingReader.close();
			}
		}
	}

	public void close() throws IOException {
		if (currentParser != null) currentParser.close();
		currentParser = null;
	}

}