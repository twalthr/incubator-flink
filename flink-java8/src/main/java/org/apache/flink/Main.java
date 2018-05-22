package org.apache.flink;

public class Main {

	public static void main(String[] args) throws Exception {
		com.sun.tools.javac.Main.main(new String[] {
//			"-proc:only",
			"-processor",
			"org.apache.flink.MyProcessor",
			"/Users/twalthr/flink/flink/flink-java8/src/main/java/org/apache/flink/examples/java8/wordcount/WordCount.java"});
	}
}
