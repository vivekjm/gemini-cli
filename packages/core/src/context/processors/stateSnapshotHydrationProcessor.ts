/**
 * @license
 * Copyright 2026 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */
import { randomUUID } from 'node:crypto';
import type { JSONSchemaType } from 'ajv';
import type { ContextProcessor, ProcessArgs } from '../pipeline.js';
import type { ContextEnvironment } from '../pipeline/environment.js';
import { type Snapshot, NodeType } from '../graph/types.js';

export interface StateSnapshotHydrationProcessorOptions {
  target?: 'incremental' | 'freeNTokens' | 'max';
}

export const StateSnapshotHydrationProcessorOptionsSchema: JSONSchemaType<StateSnapshotHydrationProcessorOptions> =
  {
    type: 'object',
    properties: {
      target: {
        type: 'string',
        enum: ['incremental', 'freeNTokens', 'max'],
        nullable: true,
      },
    },
    required: [],
  };

export function createStateSnapshotHydrationProcessor(
  id: string,
  env: ContextEnvironment,
  options: StateSnapshotHydrationProcessorOptions,
): ContextProcessor {
  return {
    id,
    name: 'StateSnapshotHydrationProcessor',
    process: async ({ targets, inbox }: ProcessArgs) => {
      if (targets.length === 0) {
        return targets;
      }

      // Determine what mode we are looking for: 'incremental' -> 'point-in-time', 'max' -> 'accumulate'
      const strategy = options.target ?? 'max';
      const expectedType =
        strategy === 'incremental' ? 'point-in-time' : 'accumulate';

      // 1. Check Inbox for a completed Snapshot (The Fast Path)
      const proposedSnapshots = inbox.getMessages<{
        newText: string;
        consumedIds: string[];
        type: string;
        timestamp: number;
      }>('PROPOSED_SNAPSHOT');

      if (proposedSnapshots.length > 0) {
        // Filter for the snapshot type that matches our processor mode
        const matchingSnapshots = proposedSnapshots.filter(
          (s) => s.payload.type === expectedType,
        );

        // Sort by newest timestamp first (we want the most accumulated snapshot)
        const sorted = [...matchingSnapshots].sort(
          (a, b) => b.timestamp - a.timestamp,
        );

        for (const proposed of sorted) {
          const { consumedIds, newText, timestamp } = proposed.payload;

          // Verify all consumed IDs still exist sequentially in targets
          const targetIds = new Set(targets.map((t) => t.id));
          const isValid = consumedIds.every((id) => targetIds.has(id));

          if (isValid) {
            // If valid, apply it!
            const newId = randomUUID();

            const snapshotNode: Snapshot = {
              id: newId,
              turnId: newId,
              type: NodeType.SNAPSHOT,
              timestamp: timestamp ?? Date.now(),
              role: 'user',
              payload: { text: newText },
              abstractsIds: consumedIds,
            };

            // Remove the consumed nodes and insert the snapshot at the earliest index
            const returnedNodes = targets.filter(
              (t) => !consumedIds.includes(t.id),
            );
            const firstRemovedIdx = targets.findIndex((t) =>
              consumedIds.includes(t.id),
            );

            if (firstRemovedIdx !== -1) {
              const idx = Math.max(0, firstRemovedIdx);
              returnedNodes.splice(idx, 0, snapshotNode);
            } else {
              returnedNodes.unshift(snapshotNode);
            }

            inbox.consume(proposed.id);
            return returnedNodes;
          }
        }
      }

      return targets;
    },
  };
}
