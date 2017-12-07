package org.apache.flink.table.client;

import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.LocalExecutor;

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

	public SqlClient(boolean isEmbedded, CliOptions options) {
		this.isEmbedded = isEmbedded;
		this.options = options;
	}

	private void start() {
		final Executor executor = new LocalExecutor();
		executor.start();

		final CliClient cli = new CliClient(executor);
		cli.attach();
	}

	private void startExecutor() {

	}

	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) {
		if (args.length < 1) {
			CliOptionsParser.printHelpClient();
		}

		switch (args[0]) {

			case MODE_EMBEDDED:
				final CliOptions options = CliOptionsParser.parseEmbeddedModeClient(args);
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
