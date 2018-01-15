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

import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SessionContext;

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
import org.jline.utils.InfoCmp;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/**
 * SQL CLI client.
 */
public class CliClient {

	private final CliTranslator translator;

	private final Terminal terminal;

	private final LineReader reader;

	private final String prompt;

	private final String rightPrompt;

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

		// initialize line reader
		final DefaultParser parser = new DefaultParser();
		parser.setEofOnEscapedNewLine(true); // allows for multi-line commands
		reader = LineReaderBuilder.builder()
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
				line = reader.readLine(prompt, rightPrompt, (MaskingCallback) null, null);
			} catch (UserInterruptException e) {
				// user cancelled application with Ctrl+C
				break;
			} catch (EndOfFileException e) {
				// user cancelled application with Ctrl+D
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
				performClearTerminal();
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

	private void performClearTerminal() {
		terminal.puts(InfoCmp.Capability.clear_screen);
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
		//translator.
	}
}
