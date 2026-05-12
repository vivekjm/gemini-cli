/**
 * @license
 * Copyright 2026 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */
import { randomUUID } from 'node:crypto';
import type { JSONSchemaType } from 'ajv';
import type {
  ContextProcessor,
  ProcessArgs,
  BackstopTargetOptions,
} from '../pipeline.js';
import type { ContextEnvironment } from '../pipeline/environment.js';
import { type ConcreteNode, type Snapshot, NodeType } from '../graph/types.js';
import {
  SnapshotGenerator,
  findLatestSnapshotBaseline,
} from '../utils/snapshotGenerator.js';
import { debugLogger } from '../../utils/debugLogger.js';

export interface StateSnapshotProcessorOptions extends BackstopTargetOptions {
  model?: string;
  systemInstruction?: string;
  maxSummaryTurns?: number;
  maxStateTokens?: number;
}

export const StateSnapshotProcessorOptionsSchema: JSONSchemaType<StateSnapshotProcessorOptions> =
  {
    type: 'object',
    properties: {
      target: {
        type: 'string',
        enum: ['incremental', 'freeNTokens', 'max'],
        nullable: true,
      },
      freeTokensTarget: { type: 'number', nullable: true },
      model: { type: 'string', nullable: true },
      systemInstruction: { type: 'string', nullable: true },
      maxSummaryTurns: { type: 'number', nullable: true },
      maxStateTokens: { type: 'number', nullable: true },
    },
    required: [],
  };

export function createStateSnapshotProcessor(
  id: string,
  env: ContextEnvironment,
  options: StateSnapshotProcessorOptions,
): ContextProcessor {
  const generator = new SnapshotGenerator(env);

  return {
    id,
    name: 'StateSnapshotProcessor',
    process: async ({ targets }: ProcessArgs) => {
      if (targets.length === 0) {
        return targets;
      }

      // Determine what mode we are looking for: 'incremental' -> 'point-in-time', 'max' -> 'accumulate'
      const strategy = options.target ?? 'max';

      // 2. The Synchronous Backstop (The Slow Path)
      let targetTokensToRemove = 0;

      if (strategy === 'incremental') {
        targetTokensToRemove = Infinity; // incremental implies removing as much as possible if no state is passed
      } else if (strategy === 'freeNTokens') {
        targetTokensToRemove = options.freeTokensTarget ?? Infinity;
      } else if (strategy === 'max') {
        targetTokensToRemove = Infinity;
      }

      let deficitAccumulator = 0;
      const nodesToSummarize: ConcreteNode[] = [];

      // Scan oldest to newest
      for (const node of targets) {
        nodesToSummarize.push(node);
        deficitAccumulator += env.tokenCalculator.getTokenCost(node);

        if (deficitAccumulator >= targetTokensToRemove) break;
      }

      if (nodesToSummarize.length < 2) return targets; // Not enough context

      let previousStateJson: string | undefined = undefined;
      let baselineIdToConsume: string | undefined = undefined;

      // Global Lookback: Find the absolute most recent snapshot anywhere in the active context
      const baseline = findLatestSnapshotBaseline(targets);

      if (baseline) {
        previousStateJson = baseline.text;
        // If the snapshot happens to be inside our summary window, remove it so the LLM doesn't read it as raw transcript
        const summaryIdx = nodesToSummarize.findIndex(
          (n) => n.id === baseline.id,
        );
        if (summaryIdx !== -1) {
          baselineIdToConsume = baseline.id;
          nodesToSummarize.splice(summaryIdx, 1);
        }
      } else {
        debugLogger.log(
          '[StateSnapshotProcessor] No previous snapshot found in context graph. Initializing new Master State baseline.',
        );
      }

      try {
        const snapshotText = await generator.synthesizeSnapshot(
          nodesToSummarize,
          previousStateJson,
          {
            maxSummaryTurns: options.maxSummaryTurns,
            maxStateTokens: options.maxStateTokens,
          },
        );
        const newId = randomUUID();
        const consumedIds = nodesToSummarize.map((n) => n.id);
        if (baselineIdToConsume && !consumedIds.includes(baselineIdToConsume)) {
          consumedIds.push(baselineIdToConsume);
        }

        const snapshotNode: Snapshot = {
          id: newId,
          turnId: newId,
          type: NodeType.SNAPSHOT,
          timestamp: nodesToSummarize[nodesToSummarize.length - 1].timestamp,
          role: 'user',
          payload: { text: snapshotText },
          abstractsIds: [...consumedIds],
        };

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

        return returnedNodes;
      } catch (e) {
        debugLogger.error('StateSnapshotProcessor failed sync backstop', e);
        return targets;
      }
    },
  };
}
