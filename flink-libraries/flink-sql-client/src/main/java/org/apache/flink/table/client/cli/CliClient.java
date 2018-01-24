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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.CliTranslator.CliResultDescriptor;
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
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp.Capability;

import java.io.IOError;
import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.jline.keymap.KeyMap.key;

/**
 * SQL CLI client.
 */
public class CliClient {

	private static final int PLAIN_MODE_WIDTH = 80;

	private static final int PLAIN_MODE_HEIGHT = 30;

	private static final int MAX_COLUMN_WIDTH = 30;

	private static final int TOTAL_HEADER_HEIGHT = 4;

	private final CliTranslator translator;

	private final Thread terminalThread;

	private final Terminal terminal;

	private final LineReader lineReader;

	private final String prompt;

	private final KeyMap<ResultOperation> resultKeys;

	private final KeyMap<ResultInputOperation> resultInputKeys;

	private final KeyMap<ResultRowOperation> resultRowKeys;

	private final BindingReader resultKeyReader;

	private final BindingReader resultInputKeyReader;

	private final BindingReader resultRowReader;

	private CliResultDescriptor resultDescriptor;

	private int resultWidth;

	private List<String[]> resultLines;

	private List<String[]> resultPreviousLines; // enables showing a diff

	private int resultRefreshInterval;

	private int resultPageCount; // 0-based because an empty result has also 1 page

	private int resultPage; // -1 = pick always the last page

	private int resultOffsetX;

	private int resultInputCursor;

	private StringBuilder resultInput;

	private ResultOptions resultOptions;

	private PageRefreshThread resultPageThread;

	private int resultSelectedLine;

	private String resultModeExitMessage;

	private static final List<Tuple2<String, Long>> REFRESH_INTERVALS = new ArrayList<>();
	private static final int LAST_PAGE = -1;
	private static final int NO_LINE_SELECTED = -1;
	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

	static {
		REFRESH_INTERVALS.add(Tuple2.of("100 ms", 100L));
		REFRESH_INTERVALS.add(Tuple2.of("500 ms", 100L));
		REFRESH_INTERVALS.add(Tuple2.of("1 s", 1_000L));
		REFRESH_INTERVALS.add(Tuple2.of("5 s", 5_000L));
		REFRESH_INTERVALS.add(Tuple2.of("10 s", 10_000L));
		REFRESH_INTERVALS.add(Tuple2.of("1 min", 60_000L));
		REFRESH_INTERVALS.add(Tuple2.of("-", 0L));
	}

	private enum ResultOperation {
		QUIT, // leave result mode
		REFRESH, // refresh current table page
		UP, // row selection up
		DOWN, // row selection down
		OPEN, // shows a full row
		GOTO, // enter table page number
		NEXT, // next table page
		PREV, // previous table page
		LAST, // last table page
		LEFT, // scroll left if row is large
		RIGHT, // scroll right if row is large
		INC_REFRESH, // increase refresh rate
		DEC_REFRESH // decrease refresh rate
	}

	private enum ResultInputOperation {
		QUIT, // leave input
		INSERT, // input
		ENTER, // apply input
		LEFT, // cursor navigation
		RIGHT, // cursor navigation
		BACKSPACE, // delete left
		DEL // delete right
	}

	private enum ResultRowOperation {
		QUIT, // leave row
		UP,
		DOWN,
		LEFT,
		RIGHT
	}

	private enum ResultOptions {
		OVERVIEW, // show available commands
		GOTO, // show field for entering page number
		ROW // show an entire row
	}

	public CliClient(SessionContext context, Executor executor) {
		this.translator = new CliTranslator(context, executor);

		terminalThread = Thread.currentThread();

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

		// create display for results
		resultKeys = bindKeys();
		resultKeyReader = new BindingReader(terminal.reader());
		resultInput = new StringBuilder();
		resultInputKeys = bindInputKeys();
		resultInputKeyReader = new BindingReader(terminal.reader());
		resultRowKeys = bindRowKeys();
		resultRowReader = new BindingReader(terminal.reader());
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
				line = lineReader.readLine(prompt, null, (MaskingCallback) null, null);
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
		final Either<CliResultDescriptor, String> result = translator.translateSelect(query);
		// print error
		if (result.isRight()) {
			terminal.writer().println(result.right());
		}
		else {
			// enter result mode
			enterResultMode(result.left());
		}
	}

	private void enterResultMode(CliResultDescriptor resultDescriptor) {
		this.resultDescriptor = resultDescriptor;

		resultRefreshInterval = 3; // every 5s
		resultPageCount = 0;
		resultPage = LAST_PAGE;
		resultOptions = ResultOptions.OVERVIEW;
		resultPageThread = new PageRefreshThread();
		resultPageThread.start();

		resultLines = Collections.emptyList();
		resultPreviousLines = Collections.emptyList();
		resultSelectedLine = NO_LINE_SELECTED;

		displayResult();

		while (isResultMode()) {
			ResultOperation op;
			try {
				op = resultKeyReader.readBinding(resultKeys, null, false);
			} catch (IOError e) {
				break;
			}

			// refresh loop
			if (op == null) {
				continue;
			}

			switch (op) {
				case QUIT:
					leaveResultMode(CliStrings.messageInfo(CliStrings.MESSAGE_RESULT_QUIT));
					break;
				case REFRESH:
					refreshResults();
					break;
				case INC_REFRESH:
					updateRefreshInterval(true);
					break;
				case DEC_REFRESH:
					updateRefreshInterval(false);
					break;
				case NEXT:
					navigatePage(true);
					break;
				case PREV:
					navigatePage(false);
					break;
				case LAST:
					navigateLastPage();
					break;
				case GOTO:
					gotoPage();
					break;
				case OPEN:
					openRow();
					break;
				case UP:
					navigateLine(true);
					break;
				case DOWN:
					navigateLine(false);
					break;
				case LEFT:
					offsetLines(true);
					break;
				case RIGHT:
					offsetLines(false);
					break;
				default:
					throw new SqlClientException("Invalid operation.");
			}

			displayResult();
		}

		clear();

		terminal.writer().println(resultModeExitMessage);
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

	private KeyMap<ResultOperation> bindKeys() {
		final KeyMap<ResultOperation> keys = new KeyMap<>();
		keys.bind(ResultOperation.QUIT, "q", "Q");
		keys.bind(ResultOperation.REFRESH, "r", "R", key(terminal, Capability.key_f5));
		keys.bind(ResultOperation.UP, "w", "W", key(terminal, Capability.key_up));
		keys.bind(ResultOperation.DOWN, "s", "S", key(terminal, Capability.key_down));
		keys.bind(ResultOperation.LEFT, "a", "A", key(terminal, Capability.key_left));
		keys.bind(ResultOperation.RIGHT, "d", "D", key(terminal, Capability.key_right));
		keys.bind(ResultOperation.OPEN, "o", "O", key(terminal, Capability.key_enter));
		keys.bind(ResultOperation.GOTO, "g", "G");
		keys.bind(ResultOperation.NEXT, "n", "N");
		keys.bind(ResultOperation.PREV, "p", "P");
		keys.bind(ResultOperation.LAST, "l", "L", key(terminal, Capability.key_end));
		keys.bind(ResultOperation.INC_REFRESH, "+");
		keys.bind(ResultOperation.DEC_REFRESH, "-");
		return keys;
	}

	private KeyMap<ResultInputOperation> bindInputKeys() {
		final KeyMap<ResultInputOperation> keys = new KeyMap<>();
		keys.setUnicode(ResultInputOperation.INSERT);
		for (char i = 32; i < 256; i++) {
            keys.bind(ResultInputOperation.INSERT, Character.toString(i));
        }
		keys.bind(ResultInputOperation.QUIT, key(terminal, Capability.key_exit));
		keys.bind(ResultInputOperation.ENTER, key(terminal, Capability.key_enter));
		return keys;
	}

	private KeyMap<ResultRowOperation> bindRowKeys() {
		final KeyMap<ResultRowOperation> keys = new KeyMap<>();
		keys.setNomatch(ResultRowOperation.QUIT);
		keys.bind(ResultRowOperation.UP, "w", "W", key(terminal, Capability.key_up));
		keys.bind(ResultRowOperation.DOWN, "s", "S", key(terminal, Capability.key_down));
		keys.bind(ResultRowOperation.LEFT, "a", "A", key(terminal, Capability.key_left));
		keys.bind(ResultRowOperation.RIGHT, "d", "D", key(terminal, Capability.key_right));
		return keys;
	}

	private List<AttributedString> computeResultHeader() {
		// compute a fixed-size header with two lines and one empty line

		// title line
		final AttributedStringBuilder titleLine = new AttributedStringBuilder();
		titleLine.style(AttributedStyle.INVERSE);
		final int totalMargin = getWidth() - CliStrings.RESULT_TITLE.length();
		final int margin = totalMargin / 2;
		repeatChar(' ', margin, titleLine);
		titleLine.append(CliStrings.RESULT_TITLE);
		repeatChar(' ', margin + (totalMargin % 2), titleLine);

		// page line
		final AttributedStringBuilder pageLine = new AttributedStringBuilder();
		pageLine.style(AttributedStyle.INVERSE);
		// left
		final String left = CliStrings.DEFAULT_MARGIN + CliStrings.RESULT_REFRESH_INTERVAL + ' ' +
			REFRESH_INTERVALS.get(resultRefreshInterval).f0;
		// middle
		final StringBuilder middleBuilder = new StringBuilder();
		middleBuilder.append(CliStrings.RESULT_PAGE);
		middleBuilder.append(' ');
		if (resultPage == LAST_PAGE) {
			middleBuilder.append(CliStrings.RESULT_LAST_PAGE);
		} else {
			middleBuilder.append(resultPage + 1);
		}
		middleBuilder.append(CliStrings.RESULT_PAGE_OF);
		middleBuilder.append(resultPageCount + 1);
		final String middle = middleBuilder.toString();
		// right
		final String right = CliStrings.RESULT_LAST_REFRESH + ' ' +
			LocalTime.now().format(FORMATTER) + CliStrings.DEFAULT_MARGIN;
		// all together
		final int totalLeftSpace = getWidth() - middle.length();
		final int leftSpace = totalLeftSpace / 2 - left.length();
		pageLine.append(left);
		repeatChar(' ', leftSpace, pageLine);
		pageLine.append(middle);
		final int rightSpacing = getWidth() - pageLine.length() - right.length();
		repeatChar(' ', rightSpacing, pageLine);
		pageLine.append(right);

		return Arrays.asList(titleLine.toAttributedString(), pageLine.toAttributedString(), AttributedString.EMPTY);
	}

	private List<AttributedString> computeResultFooter() {
		// compute a fixed-size footer with one empty line and two lines

		final AttributedStringBuilder line1 = new AttributedStringBuilder();
		final AttributedStringBuilder line2 = new AttributedStringBuilder();

		line1.append(CliStrings.DEFAULT_MARGIN);
		line2.append(CliStrings.DEFAULT_MARGIN);

		switch (resultOptions) {
			case OVERVIEW:
				final List<Tuple2<String, String>> options = getHelpOptions();
				// we assume that every options has not more than 11 characters (+ key and space)
				final int space = (getWidth() - CliStrings.DEFAULT_MARGIN.length() - (options.size() / 2 * 13)) /
					(options.size() / 2);
				final Iterator<Tuple2<String, String>> iter = options.iterator();
				while (iter.hasNext()) {
					// first line
					Tuple2<String, String> option = iter.next();
					line1.style(AttributedStyle.DEFAULT.inverse());
					line1.append(option.f0);
					line1.style(AttributedStyle.DEFAULT);
					line1.append(' ');
					line1.append(option.f1);
					repeatChar(' ', (11 - option.f1.length()) + space, line1);
					// second line
					if (iter.hasNext()) {
						option = iter.next();
						line2.style(AttributedStyle.DEFAULT.inverse());
						line2.append(option.f0);
						line2.style(AttributedStyle.DEFAULT);
						line2.append(' ');
						line2.append(option.f1);
						repeatChar(' ', (11 - option.f1.length()) + space, line2);
					}
				}
				break;

			case GOTO:
				// input
				line1.append(CliStrings.MESSAGE_ENTER_PAGE);
				line1.append(' ');
				final String input = resultInput.toString();
				line1.append(resultInput.substring(0, resultInputCursor));
				line1.style(AttributedStyle.DEFAULT.inverse());
				if (resultInputCursor < resultInput.length()) {
					line1.append(resultInput.charAt(resultInputCursor));
				} else {
					line1.append(' '); // show the cursor at the end
				}
				line1.style(AttributedStyle.DEFAULT);

				// help
				line2.append(CliStrings.MESSAGE_INPUT_HELP);
				break;
		}

		return Arrays.asList(AttributedString.EMPTY, line1.toAttributedString(), line2.toAttributedString());
	}

	private List<Tuple2<String, String>> getHelpOptions() {
		final List<Tuple2<String, String>> options = new ArrayList<>();

		options.add(Tuple2.of("Q", CliStrings.RESULT_QUIT));
		options.add(Tuple2.of("R", CliStrings.RESULT_REFRESH));

		options.add(Tuple2.of("+", CliStrings.RESULT_INC_REFRESH));
		options.add(Tuple2.of("-", CliStrings.RESULT_DEC_REFRESH));

		options.add(Tuple2.of("G", CliStrings.RESULT_GOTO));
		options.add(Tuple2.of("L", CliStrings.RESULT_LAST));

		options.add(Tuple2.of("N", CliStrings.RESULT_NEXT));
		options.add(Tuple2.of("P", CliStrings.RESULT_PREV));

		options.add(Tuple2.of("O", CliStrings.RESULT_OPEN));

		return options;
	}

	private int computeLineWidth() {
		final int[] columnWidths = computeColumnWidths();

		int width = 0;
		for (int columnWidth : columnWidths) {
			width += 1 + columnWidth; // 1 for the space between column
		}

		return width;
	}

	private int[] computeColumnWidths() {
		final String[] columnNames = resultDescriptor.getColumnNames();

		// determine maximum column width for each column
		final int[] maxWidth = new int[columnNames.length];

		for (int i = 0; i < columnNames.length; i++) {
			// schema name
			maxWidth[i] = Math.min(columnNames[i].length(), MAX_COLUMN_WIDTH);
			// values
			for (String[] values : resultLines) {
				maxWidth[i] = Math.min(Math.max(maxWidth[i], values[i].length()), MAX_COLUMN_WIDTH);
			}
		}

		return maxWidth;
	}

	private List<AttributedString> computeResultLines() {
		// compute variable-length result lines with 1 header line

		final List<AttributedString> lines = new ArrayList<>();

		final String[] columnNames = resultDescriptor.getColumnNames();
		final int[] columnWidths = computeColumnWidths();

		// schema header
		final AttributedStringBuilder schemaHeader = new AttributedStringBuilder();
		for (int i = 0; i < columnNames.length; i++) {
			schemaHeader.append(' ');
			schemaHeader.style(AttributedStyle.DEFAULT.underline());
			normalizeColumn(schemaHeader, columnNames[i], columnWidths[i]);
			schemaHeader.style(AttributedStyle.DEFAULT);
		}
		lines.add(schemaHeader.toAttributedString());

		// values
		for (int line = 0; line < resultLines.size(); line++) {
			final AttributedStringBuilder row = new AttributedStringBuilder();
			if (line == resultSelectedLine) {
				row.style(AttributedStyle.DEFAULT.inverse());
			}
			for (int col = 0; col < columnNames.length; col++) {
				row.append(' ');
				// check if value was present before last update, if not, highlight it
				// both inverse and bold does not work correctly
				if (line != resultSelectedLine && (resultLines.size() > resultPreviousLines.size() ||
						!resultLines.get(line)[col].equals(resultPreviousLines.get(line)[col]))) {
					row.style(AttributedStyle.BOLD);
					normalizeColumn(row, resultLines.get(line)[col], columnWidths[col]);
					row.style(AttributedStyle.DEFAULT);
				} else {
					normalizeColumn(row, resultLines.get(line)[col], columnWidths[col]);
				}
			}
			lines.add(row.toAttributedString());
		}

		return lines;
	}

	private void normalizeColumn(AttributedStringBuilder sb, String col, int maxWidth) {
		// limit column content
		if (col.length() > maxWidth) {
			sb.append(col, 0, maxWidth - 1);
			sb.append('~');
		} else {
			repeatChar(' ', maxWidth - col.length(), sb);
			sb.append(col);
		}
	}

	private void repeatChar(char c, int count, AttributedStringBuilder sb) {
		for (int i = 0; i < count; i++) {
			sb.append(c);
		}
	}

	private int getResultPageSize() {
		return getHeight() - 3 - 3 - 1; // height - header - footer - result schema
	}

	private boolean isResultMode() {
		return resultDescriptor != null;
	}

	private List<AttributedString> formatRow() {
		
	}

	// --------------------------------------------------------------------------------------------

	private synchronized void openRow() {
		resultOptions = ResultOptions.ROW;

		while (isResultMode() && resultOptions == ResultOptions.ROW) {
			ResultRowOperation op;
			try {
				op = resultInputKeyReader.readBinding(resultRowKeys, null, false);
			} catch (IOError e) {
				break;
			}

			if (op == null) {
				continue;
			}

			switch (op) {
				case QUIT:
					resultOptions = ResultOptions.OVERVIEW;
					break;

				case UP:
					break;

				case DOWN:
					break;

				case LEFT:
					break;

				case RIGHT:
					break;
			}
		}
	}

	private synchronized void gotoPage() {
		resultOptions = ResultOptions.GOTO;
		resultInputCursor = 0;
		resultInput.setLength(0);

		displayResult();

		while (isResultMode() && resultOptions == ResultOptions.GOTO) {
			ResultInputOperation op;
			try {
				op = resultInputKeyReader.readBinding(resultInputKeys, null, false);
			} catch (IOError e) {
				break;
			}

			if (op == null) {
				continue;
			}

			switch (op) {
				case QUIT:
					resultInput.setLength(0);
					resultOptions = ResultOptions.OVERVIEW;
					break;

				case INSERT:
					final String s = resultInputKeyReader.getLastBinding();
					resultInput.insert(resultInputCursor, s);
					resultInputCursor += s.length();
					break;

				case DEL:
					if (resultInputCursor < resultInput.length()) {
						resultInput.deleteCharAt(resultInputCursor);
					}
					break;

				case BACKSPACE:
					if (resultInputCursor > 0) {
						resultInput.deleteCharAt(resultInputCursor - 1);
					}
					break;

				case LEFT:
					if (resultInputCursor > 0) {
						resultInputCursor--;
					}
					break;

				case RIGHT:
					if (resultInputCursor < resultInput.length()) {
						resultInputCursor++;
					}
					break;

				case ENTER:
					// same as quit
					if (resultInput.length() == 0) {
						resultInput.setLength(0);
						resultOptions = ResultOptions.OVERVIEW;
					} else {
						int page = 0;
						try {
							page = Integer.parseInt(resultInput.toString().trim());
						} catch (NumberFormatException e) {
							// do nothing and let user fix the wrong input
						}
						if (page > 0 && page <= resultPageCount) {
							resultPage = page;
							updatePage();
						}
						// do nothing and let user fix the wrong input
					}
					break;
			}

			displayResult();
		}
	}

	private synchronized void displayResult() {
		if (!isResultMode()) {
			return;
		}

		final List<String> lines = new ArrayList<>();

		clear();

		final List<AttributedString> header = computeResultHeader();
		header.forEach((l) -> terminal.writer().println(l.toAnsi()));

		final List<AttributedString> result = computeResultLines();
		result.forEach((l) -> {
			final AttributedString window = l.substring(resultOffsetX, Math.min(l.length(), resultOffsetX + getWidth()));
			terminal.writer().println(window.toAnsi());
		});

		final List<AttributedString> footer = computeResultFooter();
		final int emptyHeight = getHeight() - header.size() - result.size() - footer.size();
		for (int i = 0; i < emptyHeight; i++) {
			terminal.writer().println();
		}
		footer.forEach((l) -> terminal.writer().println(l));

		terminal.flush();
	}

	private synchronized boolean refreshResults() {
		final Either<Integer, String> snapshot = translator.translateResultSnapshot(
			resultDescriptor.getResultId(), getResultPageSize());
		if (snapshot.isLeft()) {
			// update count & page
			updatePageCount(snapshot.left());
			return updatePage();
		} else {
			// an error occurred
			leaveResultMode(snapshot.right());
			return false;
		}
	}

	private synchronized void leaveResultMode(String exitMessage) {
		this.resultPageThread.cancel();
		this.resultDescriptor = null;
		this.resultModeExitMessage = exitMessage;
		terminalThread.interrupt();
	}

	private synchronized void updatePageCount(int newPageCount) {
		if (this.resultPage >= newPageCount) {
			this.resultPage = LAST_PAGE;
		}
		this.resultPageCount = newPageCount;
	}

	private synchronized void updateRefreshInterval(boolean isInc) {
		if (isInc) {
			resultRefreshInterval = (resultRefreshInterval + 1) % REFRESH_INTERVALS.size();
		} else {
			resultRefreshInterval = (resultRefreshInterval - 1) % REFRESH_INTERVALS.size();
			if (resultRefreshInterval < 0) {
				resultRefreshInterval += REFRESH_INTERVALS.size();
			}
		}
		resultPageThread.interrupt();
	}

	private synchronized boolean updatePage() {
		final int page = resultPage == LAST_PAGE ? resultPageCount : resultPage;
		final Either<List<String[]>, String> values = translator.translateResultValues(
			resultDescriptor.getResultId(), page);
		if (values.isLeft()) {
			resultPreviousLines = resultLines;
			resultLines = values.left();
			// check if selected line is still valid
			if (resultSelectedLine != NO_LINE_SELECTED) {
				if (resultSelectedLine >= resultLines.size()) {
					resultSelectedLine = NO_LINE_SELECTED;
				}
			}
			return true;
		} else {
			// error occurred
			leaveResultMode(values.right());
			return false;
		}
	}

	private synchronized void navigatePage(boolean isNext) {
		final int page = resultPage == LAST_PAGE ? resultPageCount : resultPage;
		if (isNext && page < resultPageCount) {
			resultPage = page + 1;
			updatePage();
		} else if (!isNext && page > 0) {
			resultPage = page - 1;
			updatePage();
		}
	}

	private synchronized void navigateLine(boolean isUp) {
		if (isUp) {
			if (resultSelectedLine == NO_LINE_SELECTED) {
				resultSelectedLine = resultLines.size() - 1;
			} else {
				resultSelectedLine = (resultSelectedLine - 1) % resultLines.size();
				if (resultSelectedLine < 0) {
					resultSelectedLine += resultLines.size();
				}
			}
		} else {
			if (resultSelectedLine == NO_LINE_SELECTED) {
				resultSelectedLine = 0;
			} else {
				resultSelectedLine = (resultSelectedLine + 1) % resultLines.size();
				if (resultSelectedLine < 0) {
					resultSelectedLine += resultLines.size();
				}
			}
		}
	}

	private synchronized void offsetLines(boolean isLeft) {
		final int width = getWidth();
		final int maxOffset = Math.max(0, computeLineWidth() - getWidth());
		if (isLeft) {
			if (resultOffsetX > 0) {
				resultOffsetX -= 1;
			}
		} else {
			if (resultOffsetX < maxOffset) {
				resultOffsetX += 1;
			}
		}
	}

	private synchronized void navigateLastPage() {
		resultPage = LAST_PAGE;
		updatePage();
	}

	// --------------------------------------------------------------------------------------------

	private class PageRefreshThread extends Thread {

		private boolean isRunning = true;

		@Override
		public void run() {
			while (isRunning) {
				final long interval = REFRESH_INTERVALS.get(resultRefreshInterval).f1;
				if (interval > 0) {
					// refresh according to specified interval
					if (isRunning) {
						try {
							Thread.sleep(interval);
						} catch (InterruptedException e) {
							continue;
						}
					}

					if (isRunning && refreshResults()) {
						displayResult();
					}
				} else {
					// keep the thread running but without refreshing
					try {
						Thread.sleep(100L);
					} catch (InterruptedException e) {
						// do nothing
					}
				}
			}
		}

		public void cancel() {
			isRunning = false;
			interrupt();
		}
	}
}
