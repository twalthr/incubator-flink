package org.apache.flink.table;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;

@Warmup(iterations = 3)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RowAccessBenchmark {

	private static DataType dataType = ROW(
		FIELD("a", INT().notNull()),
		FIELD("b", DOUBLE().notNull()),
		FIELD("c", BOOLEAN().notNull()),
		FIELD("d", DATE()),
		FIELD("e", DECIMAL(10, 5)));

	private static int size = dataType.getChildren().size();

	private static final int RUNS = 1000000;

	private static int swallow = 0;

	private static RowData produceNext(int i) {
		final DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(dataType);
		final Row row = Row.of(i, 2.3 + i, i % 20 == 0, i % 4 == 0 ? null : LocalDate.ofEpochDay(i), BigDecimal.valueOf(i + i));
		converter.open(RowAccessBenchmark.class.getClassLoader());
		return (RowData) converter.toInternal(row);
	}

	public static class ClassWithAccessor {
		private final RowData.FieldGetter[] fieldGetter;

		ClassWithAccessor() {
			fieldGetter = IntStream.range(0, size)
				.mapToObj(i -> RowData.createFieldGetter(dataType.getChildren().get(i).getLogicalType(), i))
				.toArray(RowData.FieldGetter[]::new);
		}

		private Object[] run(RowData rowData) {
			final Object[] o = new Object[size];
			for (int fieldPos = 0; fieldPos < size; fieldPos++) {
				o[fieldPos] = fieldGetter[fieldPos].getFieldOrNull(rowData);
			}
			return o;
		}
	}

	public static class ClassWithGetter {
		private final LogicalType[] fieldTypes;

		ClassWithGetter() {
			fieldTypes = dataType.getLogicalType().getChildren().toArray(new LogicalType[0]);
		}

		private Object[] run(RowData rowData) {
			final Object[] o = new Object[size];
			for (int fieldPos = 0; fieldPos < size; fieldPos++) {
				o[fieldPos] = RowData.get(rowData, fieldPos, fieldTypes[fieldPos]);
			}
			return o;
		}
	}

	@Benchmark
	public void testAccessor() {
		final ClassWithAccessor accessor = new ClassWithAccessor();
		for (int run = 0; run < RUNS; run++) {
			final RowData rowData = produceNext(run);
			final Object[] o = accessor.run(rowData);
			swallow(o);
		}
	}

	@Benchmark
	public void testGetter() {
		final ClassWithGetter accessor = new ClassWithGetter();
		for (int run = 0; run < RUNS; run++) {
			final RowData rowData = produceNext(run);
			final Object[] o = accessor.run(rowData);
			swallow(o);
		}
	}

	private static void swallow(Object[] o) {
		swallow += o[0].toString().length();
	}

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
			.include(RowAccessBenchmark.class.getSimpleName())
			.output("/Users/twalthr/work/tmp/Benchmark.log")
			.build();
		new Runner(options).run();
	}
}
