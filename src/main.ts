/**
 * Core application file
 *
 * - Imports necessary modules and initializes key components
 * - Sets up logging, configuration, and Deno KV store
 * - Manages ATP agent, Labeler instance, and Jetstream connection
 * - Handles cursor initialization and updates
 * - Implements error handling and graceful shutdown
 */

import { AtpAgent } from 'atproto';
import { CommitCreateEvent, CommitDeleteEvent, Jetstream } from 'jetstream';
import { Labeler } from './labeler.ts';
import {
	closeConfig,
	CONFIG,
	initializeConfig,
	setConfigValue,
} from './config.ts';
import { DidSchema } from './schemas.ts';
import { verifyKvStore } from '../scripts/kv_utils.ts';
import { AtpError, JetstreamError } from './errors.ts';
import * as log from '@std/log';
import { MetricsTracker } from './metrics.ts';

const kv = await Deno.openKv();
const logger = log.getLogger();
const processedEvents = new Set<string>();
const CACHE_CLEANUP_INTERVAL = 300000; // 5 minutes

/**
 * Main function orchestrating the application.
 * Initializes config, verifies KV store, sets up ATP agent, Labeler, and Jetstream.
 * Manages login, cursor, listeners, and shutdown handlers.
 *
 * @throws {AtpError} If ATP initialization or login fails
 * @throws {JetstreamError} If Jetstream connection fails
 */
async function main() {
	try {
		await initializeConfig();
		if (!(await verifyKvStore())) {
			throw new Error('KV store verification failed');
		}
		logger.info('KV store verified successfully');

		// Initialize cursor if not set
		const expectedCursor = Date.now() * 1000;
		if (CONFIG.CURSOR < expectedCursor) {
			logger.info(
				`Cursor needs update. Current: ${CONFIG.CURSOR}, setting to: ${expectedCursor} (${
					new Date(expectedCursor / 1000).toISOString()
				})`,
			);
			await setConfigValue('CURSOR', expectedCursor);
		} else {
			logger.info(
				`Cursor is current: ${CONFIG.CURSOR} (${
					new Date(CONFIG.CURSOR / 1000).toISOString()
				})`,
			);
		}

		const agent = new AtpAgent({ service: CONFIG.BSKY_URL });
		const metrics = new MetricsTracker(kv);
		const labeler = new Labeler(metrics);

		if (!CONFIG.BSKY_HANDLE || !CONFIG.BSKY_PASSWORD) {
			throw new AtpError(
				'BSKY_HANDLE and BSKY_PASSWORD must be set in the configuration',
			);
		}

		try {
			await agent.login({
				identifier: CONFIG.BSKY_HANDLE,
				password: CONFIG.BSKY_PASSWORD,
			});
			logger.info('Logged in to ATP successfully');
		} catch (error) {
			if (error instanceof Error) {
				throw new AtpError(`ATP login failed: ${error.message}`);
			} else {
				throw new AtpError('ATP login failed: Unknown error');
			}
		}

		await labeler.init();

		try {
			const jetstream = new Jetstream({
				wantedCollections: [CONFIG.COLLECTION],
				endpoint: CONFIG.JETSTREAM_URL,
				cursor: CONFIG.CURSOR,
			});

			setupJetstreamListeners(jetstream, labeler);

			// Set up event cache cleanup
			setInterval(() => {
				const now = Date.now();
				for (const eventId of processedEvents) {
					const [, , timeStr] = eventId.split(':');
					const eventTime = parseInt(timeStr);
					if (now - eventTime > 3600000) { // 1 hour retention
						processedEvents.delete(eventId);
					}
				}
			}, CACHE_CLEANUP_INTERVAL);

			jetstream.start();
			logger.info('Jetstream started');

			setupCursorUpdateInterval(jetstream);
			setupShutdownHandlers(labeler, jetstream);
		} catch (error) {
			if (error instanceof Error) {
				throw new JetstreamError(
					`Jetstream initialization failed: ${error.message}`,
				);
			} else {
				throw new JetstreamError(
					'Jetstream initialization failed: Unknown error',
				);
			}
		}
	} catch (error) {
		if (error instanceof AtpError || error instanceof JetstreamError) {
			logger.error(`Error in main: ${error.message}`);
		} else {
			logger.error(`Error in main: ${String(error)}`);
		}
		Deno.exit(1);
	}
}

/**
 * Generates a unique identifier for a Jetstream event.
 * Uses the event's DID, revision, and current timestamp to create a unique string.
 * Used for deduplication of events during processing.
 *
 * @param event - The Jetstream event (create or delete) to generate an ID for
 * @returns A unique string identifier for the event
 */
function generateEventId(
	event: CommitCreateEvent<string> | CommitDeleteEvent<string>,
): string {
	return `${event.did}:${event.commit.rev}:${Date.now()}`;
}

/**
 * Sets up event listeners for Jetstream.
 * Handles 'open', 'close', and 'error' events.
 * Processes 'create' events for the specified collection.
 *
 * @param jetstream - The Jetstream instance
 * @param labeler - The Labeler instance
 */
function setupJetstreamListeners(jetstream: Jetstream, labeler: Labeler) {
	let reconnectAttempt = 0;
	const MAX_RECONNECT_ATTEMPTS = 10;

	jetstream.on('open', () => {
		// Reset reconnection counter on successful connection
		reconnectAttempt = 0;
		logger.info(
			`Connected to Jetstream at ${CONFIG.JETSTREAM_URL} with cursor ${jetstream.cursor}`,
		);
	});

	jetstream.on('close', () => {
		logger.info('Jetstream connection closed');

		// Reconnection with exponential backoff
		if (reconnectAttempt < MAX_RECONNECT_ATTEMPTS) {
			const delay = Math.min(1000 * Math.pow(2, reconnectAttempt), 30000); // Cap at 30 seconds
			reconnectAttempt++;

			logger.info(
				`Attempting reconnection in ${delay}ms (attempt ${reconnectAttempt}/${MAX_RECONNECT_ATTEMPTS})`,
			);

			setTimeout(() => {
				try {
					jetstream.start();
				} catch (error) {
					logger.error(
						`Reconnection attempt failed: ${
							error instanceof Error ? error.message : String(error)
						}`,
					);
				}
			}, delay);
		} else {
			logger.error(
				'Max reconnection attempts reached. Manual intervention required.',
			);
		}
	});

	jetstream.on('error', () => {
		logger.error('Jetstream encountered a WebSocket error');
	});

	jetstream.onCreate(
		CONFIG.COLLECTION,
		async (event: CommitCreateEvent<typeof CONFIG.COLLECTION>) => {
			try {
				const eventId = generateEventId(event);
				if (processedEvents.has(eventId)) {
					logger.debug(`Skipping duplicate create event: ${eventId}`);
					return;
				}

				if (!isValidEvent(event)) {
					logger.error('Received invalid event structure:', { event });
					return;
				}

				if (event.commit?.record?.subject?.uri?.includes(CONFIG.DID)) {
					const validatedDID = DidSchema.parse(event.did);
					await labeler.handleLike(validatedDID, 'self');
					processedEvents.add(eventId);

					if (jetstream.cursor) {
						await setConfigValue('CURSOR', jetstream.cursor);
					}
				}
			} catch (error) {
				logger.error(
					`Error processing event: ${
						error instanceof Error ? error.message : String(error)
					}`,
				);
			}
		},
	);

	jetstream.onDelete(
		CONFIG.COLLECTION,
		async (event: CommitDeleteEvent<typeof CONFIG.COLLECTION>) => {
			try {
				const eventId = generateEventId(event);
				if (processedEvents.has(eventId)) {
					logger.debug(`Skipping duplicate delete event: ${eventId}`);
					return;
				}

				const { did, commit } = event;
				if (did && commit.rkey && commit.collection === CONFIG.COLLECTION) {
					const validatedDID = DidSchema.parse(did);
					await labeler.handleUnlike(validatedDID);
					processedEvents.add(eventId);

					if (jetstream.cursor) {
						await setConfigValue('CURSOR', jetstream.cursor);
					}
				}
			} catch (error) {
				logger.error(
					`Error processing delete event: ${
						error instanceof Error ? error.message : String(error)
					}`,
				);
			}
		},
	);
}

/**
 * Type guard to validate Jetstream event structure.
 * Ensures the event has the required properties for processing.
 * Checks for presence and correct types of did, commit.record.subject.uri.
 *
 * @param event - The event object to validate
 * @returns True if the event has the required structure, false otherwise
 */
function isValidEvent(event: unknown): event is {
	did: string;
	commit: {
		record: {
			subject: {
				uri: string;
			};
		};
	};
} {
	if (typeof event !== 'object' || event === null) return false;

	const e = event as Record<string, unknown>;
	return (
		typeof e.did === 'string' &&
		typeof e.commit === 'object' && e.commit !== null &&
		typeof (e.commit as Record<string, unknown>).record === 'object' &&
		(e.commit as Record<string, unknown>).record !== null &&
		typeof ((e.commit as Record<string, unknown>).record as Record<
				string,
				unknown
			>).subject === 'object' &&
		((e.commit as Record<string, unknown>).record as Record<string, unknown>)
				.subject !== null &&
		typeof (((e.commit as Record<string, unknown>).record as Record<
				string,
				unknown
			>).subject as Record<string, unknown>).uri === 'string'
	);
}

/**
 * Sets up an interval to periodically update the cursor value in the KV store.
 * This ensures we can resume from the last processed event after a restart.
 *
 * @param jetstream - The Jetstream instance to get the cursor from
 */
function setupCursorUpdateInterval(jetstream: Jetstream) {
	setInterval(async () => {
		if (jetstream.cursor) {
			logger.info(
				`Updating cursor to: ${jetstream.cursor} (${
					new Date(jetstream.cursor / 1000).toISOString()
				})`,
			);
			await setConfigValue('CURSOR', jetstream.cursor);
		}
	}, CONFIG.CURSOR_INTERVAL);
}

/**
 * Sets up handlers for SIGINT and SIGTERM signals to ensure graceful shutdown.
 * Closes all connections and cleans up resources before exiting.
 *
 * @param labeler - The Labeler instance to shut down
 * @param jetstream - The Jetstream instance to close
 */
function setupShutdownHandlers(labeler: Labeler, jetstream: Jetstream) {
	const shutdown = async () => {
		logger.info('Shutting down...');
		await labeler.shutdown();
		jetstream.close();
		await closeConfig();
		kv.close();
		Deno.exit(0);
	};

	Deno.addSignalListener('SIGINT', shutdown);
	Deno.addSignalListener('SIGTERM', shutdown);
}

// Main execution
main().catch((error) => {
	logger.critical(
		`Unhandled error in main: ${
			error instanceof Error ? error.message : String(error)
		}`,
	);
	Deno.exit(1);
});
