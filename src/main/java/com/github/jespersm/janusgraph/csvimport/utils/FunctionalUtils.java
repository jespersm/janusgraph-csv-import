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

package com.github.jespersm.janusgraph.csvimport.utils;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class FunctionalUtils {
	
	public static <TArg> Runnable bind(Consumer<TArg> consumer, TArg arg) {
        Objects.requireNonNull(consumer);
		return () -> consumer.accept(arg); 
	}

	public static <TArg1, TArg2> Consumer<TArg2> bind1(BiConsumer<TArg1, TArg2> consumer, TArg1 arg1) {
        Objects.requireNonNull(consumer);
		return (TArg2 arg2) -> consumer.accept(arg1, arg2); 
	}

	public static <TArg1, TArg2> Consumer<TArg1> bind2(BiConsumer<TArg1, TArg2> consumer, TArg2 arg2) {
        Objects.requireNonNull(consumer);
		return (TArg1 arg1) -> consumer.accept(arg1, arg2); 
	}

	public static <TArg1, TArg2, TArg3> BiConsumer<TArg2, TArg3> bind1(TriConsumer<TArg1, TArg2, TArg3> consumer, TArg1 arg1) {
        Objects.requireNonNull(consumer);
		return (TArg2 arg2, TArg3 arg3) -> consumer.accept(arg1, arg2, arg3); 
	}

	public static <TArg1, TArg2, TArg3> BiConsumer<TArg1, TArg3> bind2(TriConsumer<TArg1, TArg2, TArg3> consumer, TArg2 arg2) {
        Objects.requireNonNull(consumer);
		return (TArg1 arg1, TArg3 arg3) -> consumer.accept(arg1, arg2, arg3); 
	}

	public static <TArg1, TArg2, TArg3> BiConsumer<TArg1, TArg2> bind3(TriConsumer<TArg1, TArg2, TArg3> consumer, TArg3 arg3) {
        Objects.requireNonNull(consumer);
		return (TArg1 arg1, TArg2 arg2) -> consumer.accept(arg1, arg2, arg3); 
	}

	public static <TArg1, TArg2, TArg3> Consumer<TArg3> bind1_2(TriConsumer<TArg1, TArg2, TArg3> consumer, TArg1 arg1, TArg2 arg2) {
        Objects.requireNonNull(consumer);
		return (TArg3 arg3) -> consumer.accept(arg1, arg2, arg3); 
	}

	public static <TArg1, TArg2, TArg3> Consumer<TArg2> bind1_3(TriConsumer<TArg1, TArg2, TArg3> consumer, TArg1 arg1, TArg3 arg3) {
        Objects.requireNonNull(consumer);
		return (TArg2 arg2) -> consumer.accept(arg1, arg2, arg3); 
	}

	public static <TArg1, TArg2, TArg3> Consumer<TArg1> bind2_3(TriConsumer<TArg1, TArg2, TArg3> consumer, TArg2 arg2, TArg3 arg3) {
        Objects.requireNonNull(consumer);
		return (TArg1 arg1) -> consumer.accept(arg1, arg2, arg3); 
	}

	public static <TArg, TResult> Supplier<TResult> curry(Function<TArg, TResult> function, TArg arg) {
        Objects.requireNonNull(function);
		return () -> function.apply(arg); 
	}

	public static <TArg1, TArg2, TResult> Function<TArg2, TResult> curry1(BiFunction<TArg1, TArg2, TResult> function, TArg1 arg1) {
        Objects.requireNonNull(function);
		return (TArg2 arg2) -> function.apply(arg1, arg2); 
	}

	public static <TArg1, TArg2, TResult> Function<TArg1, TResult> curry2(BiFunction<TArg1, TArg2, TResult> function, TArg2 arg2) {
        Objects.requireNonNull(function);
		return (TArg1 arg1) -> function.apply(arg1, arg2); 
	}

	static <TObj, TArg> BiFunction<TObj, TArg, TObj> mutated(BiConsumer<TObj, TArg> setter) {
        Objects.requireNonNull(setter);
		return (obj, arg) -> {
			setter.accept(obj, arg);
			return obj;
		};
	}

	static <TObj, TArg, TRes> Function<TObj, TRes> bind(BiFunction<TObj, TArg, TRes> biFun, TArg arg) {
        Objects.requireNonNull(biFun);
		return (TObj o) -> {
			return biFun.apply(o, arg);
		};
	}

	/**
	 * Creates a mutation function which returns the same TObj after calling the setter using the supplied argument.  
	 * 
	 * Note that this is equivalent to calling <code>curry2(mutated(setter), arg)</code>
	 * 
	 * @param setter Setter which can be called with arguments TObj and TArg 
	 * @param arg The argument to be used in the setter
	 * @return Function which binds the setter and the argument.
	 */
	static <TObj, TArg> Function<TObj, TObj> withProperty(BiConsumer<TObj, TArg> setter, TArg arg) {
		return (TObj obj) -> {
			setter.accept(obj, arg);
			return obj;
		};
	}

}
