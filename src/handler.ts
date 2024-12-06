/**
 * Connection management module for Jetstream WebSocket connections.
 * Handles connection lifecycle, reconnection attempts, and error recovery.
 *
 * This module provides a robust connection management system with features for:
 * - Preventing multiple concurrent connection attempts
 * - Implementing exponential backoff with jitter for reconnection attempts
 * - Managing connection state and cleanup
 * - Providing proper error handling and logging
 * - Ensuring graceful shutdown with proper resource cleanup
 *
 * @module connection_handler
 * @license MPL-2.0
 */

import { Jetstream } from 'jetstream';
import * as log from '@std/log';
import { JetstreamError } from './errors.ts';

/**
 * Manages Jetstream WebSocket connections with automatic reconnection handling.
 * Ensures only one connection attempt is active at any time and implements
 * proper cleanup of resources during reconnection and shutdown.
 *
 * The Handler class takes responsibility for:
 * - Single connection state management
 * - Exponential backoff with jitter for reconnection
 * - Resource cleanup on shutdown
 * - Comprehensive error handling and logging
 * - Graceful shutdown coordination
 */
export class Handler {
	/** Timeout handle for reconnection attempts */
	private reconnectTimeout: number | null = null;

	/** Flag indicating if a connection attempt is in progress */
	private isConnecting = false;

	/** Flag indicating if reconnection should be attempted on failure */
	private shouldReconnect = true;

	/** Flag indicating if shutdown is in progress */
	private isShuttingDown = false;

	/** Current reconnection attempt counter */
	private reconnectAttempt = 0;

	/** Promise resolution function for connection closure */
	private closePromiseResolve: (() => void) | null = null;

	/** Promise that resolves when connection is fully closed */
	private closePromise: Promise<void> | null = null;

	/** Maximum number of reconnection attempts before requiring manual intervention */
	private readonly MAX_RECONNECT_ATTEMPTS = 10;

	/** Default timeout for shutdown operations in milliseconds */
	private readonly SHUTDOWN_TIMEOUT = 5000;

	/** Logger instance for connection-related events */
	private readonly logger = log.getLogger();

	/**
	 * Creates a new Handler instance and sets up event handlers.
	 *
	 * @param jetstream - The Jetstream instance to manage
	 * @param baseDelay - Base delay in milliseconds between reconnection attempts (default: 30000)
	 * @param maxDelay - Maximum delay in milliseconds between reconnection attempts (default: 60000)
	 */
	constructor(
		private readonly jetstream: Jetstream<string, string>,
		private readonly baseDelay: number = 30000,
		private readonly maxDelay: number = 60000,
	) {
		this.setupEventHandlers();
	}

	/**
	 * Starts the Jetstream connection with proper state management.
	 * Prevents multiple concurrent connection attempts.
	 *
	 * @throws {JetstreamError} If connection fails after max reconnection attempts
	 */
	public async start(): Promise<void> {
		if (this.isConnecting) {
			this.logger.debug('Connection attempt already in progress');
			return;
		}

		try {
			this.isConnecting = true;
			this.shouldReconnect = true;
			await this.connect();
		} finally {
			this.isConnecting = false;
		}
	}

	/**
	 * Initiates a graceful shutdown of the connection.
	 * Returns a promise that resolves when the connection is fully closed.
	 * Includes a timeout to prevent hanging if closure confirmation is not received.
	 *
	 * @returns Promise that resolves when shutdown is complete
	 */
	public shutdown(): Promise<void> {
		if (this.isShuttingDown) {
			return this.closePromise || Promise.resolve();
		}

		this.isShuttingDown = true;
		this.shouldReconnect = false;
		this.cleanup();

		// Create a promise that will resolve when the close event is received
		this.closePromise = new Promise((resolve) => {
			this.closePromiseResolve = resolve;

			// Set a timeout in case the close event is never received
			const timeoutId = setTimeout(() => {
				this.logger.warn('Connection close timed out during shutdown');
				if (this.closePromiseResolve) {
					this.closePromiseResolve();
					this.closePromiseResolve = null;
				}
			}, this.SHUTDOWN_TIMEOUT);

			// Ensure timeout is cleared if close event is received
			this.closePromise?.finally(() => clearTimeout(timeoutId));
		});

		this.jetstream.close();
		return this.closePromise;
	}

	/**
	 * Cleans up connection-related resources.
	 * Cancels any pending reconnection timeouts.
	 */
	private cleanup(): void {
		if (this.reconnectTimeout !== null) {
			clearTimeout(this.reconnectTimeout);
			this.reconnectTimeout = null;
		}
	}

	/**
	 * Calculates the delay for the next reconnection attempt.
	 * Uses exponential backoff with random jitter to prevent thundering herd problems.
	 *
	 * @returns The delay in milliseconds before the next reconnection attempt
	 */
	private calculateDelay(): number {
		const baseDelay = this.baseDelay + (3000 * this.reconnectAttempt);
		const jitter = Math.random() * 1000;
		return Math.min(baseDelay + jitter, this.maxDelay);
	}

	/**
	 * Initiates a connection attempt.
	 * Handles connection errors and initiates reconnection if appropriate.
	 *
	 * @throws {JetstreamError} If connection fails and reconnection is disabled
	 */
	private async connect(): Promise<void> {
		try {
			this.cleanup();
			this.jetstream.start();
			this.reconnectAttempt = 0;

			this.logger.info(
				`Connected to Jetstream with cursor ${this.jetstream.cursor}`,
			);
		} catch (error) {
			this.logger.error(
				`Connection attempt failed: ${
					error instanceof Error ? error.message : String(error)
				}`,
			);
			await this.handleConnectionError(error);
		}
	}

	/**
	 * Handles connection errors and manages reconnection attempts.
	 * Implements exponential backoff with jitter for reconnection timing.
	 *
	 * @param error - The error that caused the connection failure
	 * @throws {JetstreamError} If max reconnection attempts are reached
	 */
	private async handleConnectionError(error: unknown): Promise<void> {
		if (!this.shouldReconnect) {
			this.logger.info('Connection terminated, reconnection disabled');
			return;
		}

		this.logger.error(
			`Connection failed: ${
				error instanceof Error ? error.message : String(error)
			}`,
		);

		if (this.reconnectAttempt >= this.MAX_RECONNECT_ATTEMPTS) {
			const msg =
				'Max reconnection attempts reached. Manual intervention required.';
			this.logger.error(msg);
			throw new JetstreamError(msg);
		}

		const delay = this.calculateDelay();
		this.reconnectAttempt++;

		this.logger.info(
			`Scheduling reconnection in ${delay}ms (attempt ${this.reconnectAttempt}/${this.MAX_RECONNECT_ATTEMPTS})`,
		);

		await new Promise<void>((resolve) => {
			this.reconnectTimeout = setTimeout(async () => {
				try {
					await this.connect();
					resolve();
				} catch (reconnectError) {
					this.logger.error(
						`Reconnection attempt failed: ${
							reconnectError instanceof Error
								? reconnectError.message
								: String(reconnectError)
						}`,
					);
					resolve();
				}
			}, delay);
		});
	}

	/**
	 * Sets up event handlers for the Jetstream instance.
	 * Handles 'open', 'close', and 'error' events with appropriate logging
	 * and reconnection logic.
	 */
	public setupEventHandlers(): void {
		this.jetstream.on('open', () => {
			this.reconnectAttempt = 0;
			this.logger.info(
				`Connected to Jetstream with cursor ${this.jetstream.cursor}`,
			);
		});

		this.jetstream.on('close', async () => {
			this.logger.info('Jetstream connection closed');

			// If we're shutting down, resolve the close promise
			if (this.isShuttingDown && this.closePromiseResolve) {
				this.closePromiseResolve();
				this.closePromiseResolve = null;
				return;
			}

			// Otherwise attempt reconnection if enabled
			if (this.shouldReconnect) {
				await this.start();
			}
		});

		this.jetstream.on('error', () => {
			this.logger.error('Jetstream encountered a WebSocket error');
			// Let the close handler manage reconnection
		});
	}
}
