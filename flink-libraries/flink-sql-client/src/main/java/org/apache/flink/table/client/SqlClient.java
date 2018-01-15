package org.apache.flink.table.client;

import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.LocalExecutor;
import org.apache.flink.table.client.gateway.SessionContext;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

/**
 * SQL Client for submitting SQL statements. The client can be executed in two
 * modes: a gateway and embedded mode.
 *
 * - In gateway mode, the SQL CLI client connects to the REST API of the gateway and allows for
 * managing queries via console.
 *
 * - In embedded mode, the SQL CLI is tightly coupled with the executor in a common process. This
 * allows for submitting jobs without having to start an additional components.
 */
public class SqlClient {

	private final boolean isEmbedded;
	private final CliOptions options;

	public static final String MODE_EMBEDDED = "embedded";
	public static final String MODE_GATEWAY = "gateway";

	public static final String DEFAULT_SESSION_ID = "default";

	public SqlClient(boolean isEmbedded, CliOptions options) {
		this.isEmbedded = isEmbedded;
		this.options = options;
	}

	private void start() {
		if (isEmbedded) {
			// create local executor with default environment
			final Environment defaultEnv = readEnvironment(options.getDefaults());
			final Executor executor = new LocalExecutor(defaultEnv);
			executor.start();

			// create CLI client with session environment
			final Environment sessionEnv = readEnvironment(options.getEnvironment());
			final SessionContext sessionContext;
			if (options.getSessionId() == null) {
				sessionContext = new SessionContext(DEFAULT_SESSION_ID, sessionEnv);
			} else {
				sessionContext = new SessionContext(options.getSessionId(), sessionEnv);
			}
			final CliClient cli = new CliClient(sessionContext, executor);
			cli.attach();
		} else {
			throw new SqlClientException("Gateway mode is not supported yet.");
		}
	}

	// --------------------------------------------------------------------------------------------

	private static Environment readEnvironment(URL envUrl) {
		// use an empty environment by default
		if (envUrl == null) {
			return new Environment();
		}

		try {
			return Environment.parse(envUrl);
		} catch (IOException e) {
			throw new SqlClientException("Could not read environment file at: " + envUrl, e);
		}
	}

	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) {
		if (args.length < 1) {
			CliOptionsParser.printHelpClient();
		}

		switch (args[0]) {

			case MODE_EMBEDDED:
				// remove mode
				final String[] modeArgs = Arrays.copyOfRange(args, 1, args.length);
				final CliOptions options = CliOptionsParser.parseEmbeddedModeClient(modeArgs);
				if (options.isPrintHelp()) {
					CliOptionsParser.printHelpEmbeddedModeClient();
				} else {
					final SqlClient client = new SqlClient(true, options);
					client.start();
				}
				break;

			case MODE_GATEWAY:
				throw new SqlClientException("Gateway mode is not supported yet.");

			default:
				CliOptionsParser.printHelpClient();
		}
	}
}
