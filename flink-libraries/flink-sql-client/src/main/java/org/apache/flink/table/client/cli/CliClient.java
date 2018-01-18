/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.client.cli;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.types.Either;

import org.jline.keymap.BindingReader;
import org.jline.keymap.KeyMap;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp.Capability;

import java.io.IOError;
import java.io.IOException;
import java.text.AttributedString;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.jline.keymap.KeyMap.ctrl;
import static org.jline.keymap.KeyMap.key;

/**
 * SQL CLI client.
 */
public class CliClient {

	private static final int PLAIN_MODE_WIDTH = 80;

	private static final int PLAIN_MODE_HEIGHT = 30;

	private final CliTranslator translator;

	private final Terminal terminal;

	private final LineReader lineReader;

	private final String prompt;

	private final String rightPrompt;

	private final KeyMap<ResultOperation> resultKeys;

	private final BindingReader resultKeyReader;

	private final List<String> resultLines;

	private static final List<Tuple2<String, Long>> REFRESH_INTERVALS = new ArrayList<>();

	static {
		REFRESH_INTERVALS.add(Tuple2.of("-", 0L));
		REFRESH_INTERVALS.add(Tuple2.of("100 ms", 100L));
		REFRESH_INTERVALS.add(Tuple2.of("500 ms", 100L));
		REFRESH_INTERVALS.add(Tuple2.of("1 s", 1_000L));
		REFRESH_INTERVALS.add(Tuple2.of("5 s", 5_000L));
		REFRESH_INTERVALS.add(Tuple2.of("10 s", 10_000L));
		REFRESH_INTERVALS.add(Tuple2.of("1 min", 60_000L));
	}

	private enum ResultOperation {
		QUIT, // leave result mode
		REFRESH, // refresh current table page
		GOTO, // enter table page number
		NEXT, // next table page
		PREV, // previous table page
		LAST, // last table page
		FIRST, // first table page
		SEARCH, // search for string in current page and following pages
		SEARCH_NEXT, // continue search
		LEFT, // scroll left if row is large
		RIGHT, // scroll right if row is large
		INC_REFRESH, // increase refresh rate
		DEC_REFRESH // decrease refresh rate
	}

	public CliClient(SessionContext context, Executor executor) {
		this.translator = new CliTranslator(context, executor);

		try {
			// initialize terminal
			terminal = TerminalBuilder.builder()
				.name(CliStrings.CLI_NAME)
				.build();
		} catch (IOException e) {
			throw new SqlClientException("Error opening command line interface.", e);
		}

		// initialize line lineReader
		final DefaultParser parser = new DefaultParser();
		parser.setEofOnEscapedNewLine(true); // allows for multi-line commands
		lineReader = LineReaderBuilder.builder()
			.terminal(terminal)
			.appName(CliStrings.CLI_NAME)
			.parser(parser)
			.build();

		// create prompt
		prompt = new AttributedStringBuilder()
			.style(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
			.append("Flink SQL")
			.style(AttributedStyle.DEFAULT)
			.append("> ")
			.toAnsi();

		rightPrompt = new AttributedStringBuilder()
			.style(AttributedStyle.DEFAULT.background(AttributedStyle.RED))
			.append(LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")))
			.toAnsi();

		// create display for results
		resultKeys = bindKeys();
		resultKeyReader = new BindingReader(terminal.reader());
		resultLines = new ArrayList<>();
	}

	public void attach() {
		// print welcome
		terminal.writer().append(CliStrings.MESSAGE_WELCOME);

		// begin reading loop
		while (true) {
			// make some space to previous command
			terminal.writer().append("\n");
			terminal.flush();

			String line;
			try {
				line = lineReader.readLine(prompt, rightPrompt, (MaskingCallback) null, null);
			} catch (UserInterruptException e) {
				// user cancelled application with Ctrl+C
				break;
			} catch (EndOfFileException e) {
				// user cancelled application with Ctrl+D
				break;
			} catch (IOError e) {
				// user cancelled application (e.g. IntelliJ cancel)
				break;
			} catch (Throwable t) {
				throw new SqlClientException("Could not read from command line.", t);
			}
			if (line == null || line.equals("")) {
				continue;
			}

			// normalize string to figure out the main command
			final String[] tokenized = line.trim().split("\\s+");

			if (commandMatch(tokenized, CliStrings.COMMAND_QUIT)) {
				terminal.writer().append(CliStrings.MESSAGE_QUIT);
				terminal.flush();
				break;
			} else if (commandMatch(tokenized, CliStrings.COMMAND_CLEAR)) {
				clear();
			} else if (commandMatch(tokenized, CliStrings.COMMAND_HELP)) {
				performHelp();
			} else if (commandMatch(tokenized, CliStrings.COMMAND_SHOW_TABLES)) {
				performShowTables();
			} else if (commandMatch(tokenized, CliStrings.COMMAND_DESCRIBE)) {
				performDescribe(removeCommand(tokenized, CliStrings.COMMAND_DESCRIBE));
			} else if (commandMatch(tokenized, CliStrings.COMMAND_EXPLAIN)) {
				performExplain(removeCommand(tokenized, CliStrings.COMMAND_EXPLAIN));
			} else if (commandMatch(tokenized, CliStrings.COMMAND_SELECT)) {
				performSelect(line);
			} else {
				terminal.writer().println(CliStrings.messageError(CliStrings.MESSAGE_UNKNOWN_SQL));
			}
		}
	}

	private boolean commandMatch(String[] tokens, String command) {
		// check for statement match
		final String[] commandTokens = command.split(" ");
		if (tokens.length < commandTokens.length) {
			return false;
		}
		for (int i = 0; i < commandTokens.length; i++) {
			if (!tokens[i].equalsIgnoreCase(commandTokens[i])) {
				return false;
			}
		}
		return true;
	}

	private String removeCommand(String[] tokens, String command) {
		final String[] commandTokens = command.split(" ");
		return String.join(" ", Arrays.copyOfRange(tokens, commandTokens.length, tokens.length));
	}

	private void performHelp() {
		terminal.writer().println(CliStrings.MESSAGE_HELP);
	}

	private void performShowTables() {
		terminal.writer().println(translator.translateShowTables());
	}

	private void performDescribe(String argument) {
		terminal.writer().println(translator.translateDescribeTable(argument));
	}

	private void performExplain(String argument) {
		terminal.writer().println(translator.translateExplainTable(argument));
	}

	private void performSelect(String query) {
		final Either<String, String> result = translator.translateSelect(query);
		// print error
		if (result.isRight()) {
			terminal.writer().println(result.right());
		}
		else {
			// enter result mode
			final String resultId = result.left();

			displayResult();

			while (true) {
				final ResultOperation op = resultKeyReader.readBinding(resultKeys);
				switch (op) {
					case QUIT:
						break;
					default:
						throw new SqlClientException("Invalid operation.");
				}
			}

		}
	}

	private void clear() {
		if (isPlainMode()) {
			for (int i = 0; i < 200; i++) { // large number of empty lines
				terminal.writer().println();
			}
		} else {
			terminal.puts(Capability.clear_screen);
		}
	}

	private boolean isPlainMode() {
		// check if terminal width can be determined
		// e.g. IntelliJ IDEA terminal supports only plain mode
		return terminal.getWidth() == 0 && terminal.getHeight() == 0;
	}

	private int getWidth() {
		if (isPlainMode()) {
			return PLAIN_MODE_WIDTH;
		}
		return terminal.getWidth();
	}

	private int getHeight() {
		if (isPlainMode()) {
			return PLAIN_MODE_HEIGHT;
		}
		return terminal.getHeight();
	}

	private void displayResult() {
		final List<String> lines = new ArrayList<>();

		clear();

		// add header
		lines.addAll(computeResultHeader());

		// main content
		lines.addAll(resultLines);

		// add footer
		lines.addAll(computeResultFooter());

		lines.forEach((l) -> terminal.writer().println(l));

		terminal.flush();
	}

	private KeyMap<ResultOperation> bindKeys() {
		final KeyMap<ResultOperation> keys = new KeyMap<>();
		keys.bind(ResultOperation.QUIT, "q", "Q");
		keys.bind(ResultOperation.REFRESH, "r", "R", key(terminal, Capability.key_f5));
		keys.bind(ResultOperation.GOTO, "g", "G");
		keys.bind(ResultOperation.NEXT, "n", "N", key(terminal, Capability.key_down));
		keys.bind(ResultOperation.PREV, "p", "P", key(terminal, Capability.key_up));
		keys.bind(ResultOperation.LAST, "l", "L", key(terminal, Capability.key_end));
		keys.bind(ResultOperation.FIRST, "f", "F", key(terminal, Capability.key_beg));
		keys.bind(ResultOperation.SEARCH, "s", "S", ctrl('f'));
		keys.bind(ResultOperation.SEARCH_NEXT, key(terminal, Capability.key_enter), key(terminal, Capability.key_f5));
		keys.bind(ResultOperation.LEFT, key(terminal, Capability.key_left));
		keys.bind(ResultOperation.RIGHT, key(terminal, Capability.key_right));
		keys.bind(ResultOperation.INC_REFRESH, "+");
		keys.bind(ResultOperation.DEC_REFRESH, "-");
		return keys;
	}

	private List<String> computeResultHeader() {

		// title line
		AttributedStringBuilder sb = new AttributedStringBuilder();
		sb.style(AttributedStyle.INVERSE);
		final int totalMargin = getWidth() - CliStrings.RESULT_TITLE.length();
		final int margin = totalMargin / 2;
		for (int i = 0; i < margin; i++) {
			sb.append(' ');
		}
		sb.append(CliStrings.RESULT_TITLE);
		for (int i = 0; i < margin + (totalMargin % 2); i++) {
			sb.append(' ');
		}
		final String titleLine = sb.toAnsi();

		// page line
		sb = new AttributedStringBuilder();
		sb.style(AttributedStyle.INVERSE);
		sb.append("  ");
		sb.append(CliStrings.RESULT_REFRESH_INTERVAL);
		final String pageLine = sb.toAnsi();

		return Arrays.asList(titleLine, pageLine);
	}

	private List<String> computeResultFooter() {
		return Collections.emptyList();
	}






























}
