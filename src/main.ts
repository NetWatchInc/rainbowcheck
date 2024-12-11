/**
 * Core application file for event processing and the labeling system.
 *
 * This module serves as the primary entry point for the application, orchestrating
 * all major components and managing the application lifecycle. It implements a
 * robust event processing system that integrates with the AT Protocol Jetstream
 * service for real-time event handling.
 *
 * Key responsibilities:
 * - Configuration initialization and validation
 * - KV store setup and management
 * - ATP authentication and session management
 * - Jetstream connection handling and event processing
 * - Event deduplication and cursor management
 * - Graceful shutdown coordination
 *
 * The application maintains persistent connections and ensures proper cleanup
 * of resources during both normal operation and shutdown scenarios.
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
import { Handler } from './handler.ts';

/** Persistent key-value store for application state and data */
const kv = await Deno.openKv();

/** Application-wide logger instance */
const logger = log.getLogger();

/** Set for tracking processed events to prevent duplicates */
const processedEvents = new Set<string>();

/** Interval for cleaning up expired events from the deduplication cache (5 minutes) */
const CACHE_CLEANUP_INTERVAL = 300000;

/** Duration to retain processed events in the cache (1 hour) */
const EVENT_RETENTION_DURATION = 3600000;

/**
 * Main function orchestrating the application lifecycle.
 * Initializes all components and manages the core event processing loop.
 *
 * This function:
 * 1. Initializes and validates configuration
 * 2. Sets up and verifies the KV store
 * 3. Authenticates with ATP
 * 4. Establishes Jetstream connection
 * 5. Sets up event processing and monitoring
 *
 * @throws {AtpError} If ATP initialization or authentication fails
 * @throws {JetstreamError} If Jetstream connection or initialization fails
 */
async function main() {
	try {
		await initializeConfig();
		if (!(await verifyKvStore())) {
			throw new Error('KV store verification failed');
		}
		logger.info('KV store verified successfully');

		// Ensure cursor is properly initialized
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

		// Initialize core services
		const agent = new AtpAgent({ service: CONFIG.BSKY_URL });
		const metrics = new MetricsTracker(kv);
		const labeler = new Labeler(metrics);

		// Validate required authentication configuration
		if (!CONFIG.BSKY_HANDLE || !CONFIG.BSKY_PASSWORD) {
			throw new AtpError(
				'BSKY_HANDLE and BSKY_PASSWORD must be set in the configuration',
			);
		}

		// Authenticate with ATP service
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
			// Initialize Jetstream connection
			const jetstream = new Jetstream({
				wantedCollections: [CONFIG.COLLECTION],
				endpoint: CONFIG.JETSTREAM_URL,
				cursor: CONFIG.CURSOR,
			});

			// Configure event handling
			setupJetstreamListeners(jetstream, labeler);

			// Configure cache cleanup
			setInterval(() => {
				const now = Date.now();
				for (const eventId of processedEvents) {
					const [, , timeStr] = eventId.split(':');
					const eventTime = parseInt(timeStr);
					if (now - eventTime > EVENT_RETENTION_DURATION) {
						processedEvents.delete(eventId);
					}
				}
			}, CACHE_CLEANUP_INTERVAL);

			// Initialize and start connection management
			const handler = new Handler(jetstream);
			await handler.start();
			logger.info('Jetstream started with connection management');

			setupCursorUpdateInterval(jetstream, handler);
			setupShutdownHandlers(labeler, handler);
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
 * Generates a unique identifier for event deduplication.
 * Combines the event's DID, revision, and timestamp to create a unique string.
 *
 * @param event - The Jetstream event requiring a unique identifier
 * @returns A unique string identifier for the event
 */
function generateEventId(
	event: CommitCreateEvent<string> | CommitDeleteEvent<string>,
): string {
	return `${event.did}:${event.commit.rev}:${Date.now()}`;
}

/**
 * Configures event listeners for the Jetstream connection.
 * Sets up handlers for processing create and delete events.
 *
 * This function implements event deduplication and ensures proper
 * processing order for incoming events. It validates event structure
 * and maintains cursor state for reliable event processing.
 *
 * @param jetstream - The Jetstream instance to configure
 * @param labeler - The Labeler instance for processing events
 */
function setupJetstreamListeners(
	jetstream: Jetstream<string, string>,
	labeler: Labeler,
) {
	// Configure create event handling
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
					await kv.set(['rkeys', event.commit.rkey], validatedDID);
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

	// Configure delete event handling
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
					const result = await kv.get(['rkeys', commit.rkey]);
					if (result.value) {
						const validatedDID = DidSchema.parse(did);
						await labeler.handleUnlike(validatedDID);
						await kv.delete(['rkeys', commit.rkey]);
						processedEvents.add(eventId);

						if (jetstream.cursor) {
							await setConfigValue('CURSOR', jetstream.cursor);
						}
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
 * Type guard for validating Jetstream event structure.
 * Ensures events contain all required properties with correct types.
 *
 * @param event - The event object to validate
 * @returns True if the event has valid structure, false otherwise
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
 * Configures periodic cursor updates to maintain processing state.
 * Updates the cursor value in the KV store at regular intervals.
 *
 * @param jetstream - The Jetstream instance providing cursor values
 */
function setupCursorUpdateInterval(
	jetstream: Jetstream<string, string>,
	handler: Handler,
) {
	setInterval(async () => {
		if (jetstream.cursor && handler.connected) {
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
 * Configures handlers for graceful application shutdown.
 * Ensures proper cleanup of resources and connections on exit.
 *
 * This function sets up handlers for SIGINT and SIGTERM signals,
 * coordinating the shutdown sequence across all components.
 *
 * @param labeler - The Labeler instance to shut down
 * @param handler - The Handler instance managing Jetstream connection
 */
function setupShutdownHandlers(
	labeler: Labeler,
	handler: Handler,
) {
	let isShuttingDown = false;

	const shutdown = async () => {
		if (isShuttingDown) {
			return;
		}
		isShuttingDown = true;

		logger.info('Initiating shutdown sequence...');

		try {
			await Promise.race([
				handler.shutdown(),
				new Promise((resolve) => setTimeout(resolve, 7000)),
			]);
			await labeler.shutdown();
			await closeConfig();
			kv.close();
			logger.info('Shutdown completed successfully');
		} catch (error) {
			logger.error(
				`Error during shutdown: ${
					error instanceof Error ? error.message : String(error)
				}`,
			);
		} finally {
			Deno.exit(0);
		}
	};

	Deno.addSignalListener('SIGINT', shutdown);
	Deno.addSignalListener('SIGTERM', shutdown);
}

// Application entry point
main().catch((error) => {
	logger.critical(
		`Unhandled error in main: ${
			error instanceof Error ? error.message : String(error)
		}`,
	);
	Deno.exit(1);
});
