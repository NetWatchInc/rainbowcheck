/**
 * Label definitions
 *
 * This module defines the source of truth for available labels.
 */

/**
 * Readonly array of Label objects that are validated against the schema.
 * Each label is comprised of rkey, identifier, and category.
 */

export const LABELS = [
	{
		rkey: 'self',
		identifier: 'rcheck',
		category: 'rcheck',
	},
] as const;
