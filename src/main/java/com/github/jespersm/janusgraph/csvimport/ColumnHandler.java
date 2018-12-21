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

public interface ColumnHandler<T> {
	enum Tag { ID, INDEX, UNIQUE, START_ID, END_ID, TYPE, IGNORE, DATA }
	
	Class<T> getDatatype();
	T convert(String raw);
	String getName();
	Tag getTag();
}
