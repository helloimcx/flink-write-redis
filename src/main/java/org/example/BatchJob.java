package org.example;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//通过字符串构建数据集
		DataSet<String> text = env.fromElements(
				"风急天高猿啸哀，渚清沙白鸟飞回。" +
						"无边落木萧萧下，不尽长江滚滚来。" +
						"万里悲秋常作客，百年多病独登台。" +
						"艰难苦恨繁霜鬓，潦倒新停浊酒杯。");
		// 分割字符串、按照key进行分组、统计相同的key个数
		DataSet<Tuple2<String, Integer>> wordCounts = text
				.flatMap(new LineSplitter())
				.groupBy(0)
				.sum(1);
		// 打印
		wordCounts.print();
	}

	// 分割字符串的方法
	public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
			for (String word : line.split("")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}
}
