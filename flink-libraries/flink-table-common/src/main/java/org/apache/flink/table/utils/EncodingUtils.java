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

package org.apache.flink.table.utils;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

/**
 * General utilities for string-encoding. This class is used to avoid additional dependencies
 * to other projects.
 */
public abstract class EncodingUtils {

	private EncodingUtils() {
		// do not instantiate
	}

	// --------------------------------------------------------------------------------------------
	// Java String Escaping
	//
	// copied from org.apache.commons.lang.StringEscapeUtils (commons-lang:commons-lang:2.4)
	// --------------------------------------------------------------------------------------------

	/**
	 * <p>Escapes the characters in a <code>String</code> using Java String rules.</p>
	 * <p>
	 * <p>Deals correctly with quotes and control-chars (tab, backslash, cr, ff, etc.) </p>
	 * <p>
	 * <p>So a tab becomes the characters <code>'\\'</code> and
	 * <code>'t'</code>.</p>
	 * <p>
	 * <p>The only difference between Java strings and JavaScript strings
	 * is that in JavaScript, a single quote must be escaped.</p>
	 * <p>
	 * <p>Example:
	 * <pre>
	 * input string: He didn't say, "Stop!"
	 * output string: He didn't say, \"Stop!\"
	 * </pre>
	 * </p>
	 *
	 * @param str String to escape values in, may be null
	 * @return String with escaped values, <code>null</code> if null string input
	 */
	public static String escapeJava(String str) {
		return escapeJavaStyleString(str, false);
	}

	private static String escapeJavaStyleString(String str, boolean escapeSingleQuotes) {
		if (str == null) {
			return null;
		}
		try {
			StringWriter writer = new StringWriter(str.length() * 2);
			escapeJavaStyleString(writer, str, escapeSingleQuotes);
			return writer.toString();
		} catch (IOException ioe) {
			// this should never ever happen while writing to a StringWriter
			ioe.printStackTrace();
			return null;
		}
	}

	private static void escapeJavaStyleString(Writer out, String str, boolean escapeSingleQuote) throws IOException {
		if (out == null) {
			throw new IllegalArgumentException("The Writer must not be null");
		}
		if (str == null) {
			return;
		}
		int sz;
		sz = str.length();
		for (int i = 0; i < sz; i++) {
			char ch = str.charAt(i);

			// handle unicode
			if (ch > 0xfff) {
				out.write("\\u" + hex(ch));
			} else if (ch > 0xff) {
				out.write("\\u0" + hex(ch));
			} else if (ch > 0x7f) {
				out.write("\\u00" + hex(ch));
			} else if (ch < 32) {
				switch (ch) {
					case '\b':
						out.write('\\');
						out.write('b');
						break;
					case '\n':
						out.write('\\');
						out.write('n');
						break;
					case '\t':
						out.write('\\');
						out.write('t');
						break;
					case '\f':
						out.write('\\');
						out.write('f');
						break;
					case '\r':
						out.write('\\');
						out.write('r');
						break;
					default:
						if (ch > 0xf) {
							out.write("\\u00" + hex(ch));
						} else {
							out.write("\\u000" + hex(ch));
						}
						break;
				}
			} else {
				switch (ch) {
					case '\'':
						if (escapeSingleQuote) {
							out.write('\\');
						}
						out.write('\'');
						break;
					case '"':
						out.write('\\');
						out.write('"');
						break;
					case '\\':
						out.write('\\');
						out.write('\\');
						break;
					case '/':
						out.write('\\');
						out.write('/');
						break;
					default:
						out.write(ch);
						break;
				}
			}
		}
	}

	private static String hex(char ch) {
		return Integer.toHexString(ch).toUpperCase();
	}
}
