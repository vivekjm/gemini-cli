/**
 * @license
 * Copyright 2026 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, it, expect } from 'vitest';
import { ContextGraphBuilder } from './toGraph.js';
import type { Content } from '@google/genai';
import type { BaseConcreteNode } from './types.js';

describe('ContextGraphBuilder', () => {
  describe('toGraph', () => {
    it('should skip legacy <session_context> headers even if they appear later in the history', () => {
      const history: Content[] = [
        { role: 'user', parts: [{ text: 'Message 1' }] },
        { role: 'model', parts: [{ text: 'Reply 1' }] },
        {
          role: 'user',
          parts: [
            {
              text: '<session_context>\nThis is the Gemini CLI\nSome context...',
            },
          ],
        },
        { role: 'user', parts: [{ text: 'Message 2' }] },
      ];

      const builder = new ContextGraphBuilder();
      const nodes = builder.processHistory(history);

      // We expect the first two messages and the last one to be present
      // The session context message should be filtered out
      expect(nodes.length).toBe(3);
      expect((nodes[0] as BaseConcreteNode).payload.text).toBe('Message 1');
      expect((nodes[1] as BaseConcreteNode).payload.text).toBe('Reply 1');
      expect((nodes[2] as BaseConcreteNode).payload.text).toBe('Message 2');
    });

    it('should generate completely deterministic graph structure and UUIDs across JSON serialization cycles', () => {
      const complexHistory: Content[] = [
        { role: 'user', parts: [{ text: 'Step 1: complex analysis' }] },
        {
          role: 'model',
          parts: [
            { text: 'Thinking about the tool to use.' },
            {
              functionCall: {
                name: 'fetch_data',
                args: { query: 'test data' },
              },
            },
          ],
        },
        {
          role: 'user',
          parts: [
            {
              functionResponse: {
                name: 'fetch_data',
                response: { status: 'success', data: [1, 2, 3] },
              },
            },
          ],
        },
        { role: 'model', parts: [{ text: 'Analysis complete.' }] },
      ];

      // 1. Initial Graph Generation
      const builder1 = new ContextGraphBuilder();
      const nodes1 = builder1.processHistory(complexHistory);

      // 2. Serialize and Deserialize (Simulating saving and loading from disk)
      const serializedHistory = JSON.stringify(complexHistory);
      const parsedHistory = JSON.parse(serializedHistory) as Content[];

      // 3. Second Graph Generation from parsed JSON
      const builder2 = new ContextGraphBuilder();
      const nodes2 = builder2.processHistory(parsedHistory);

      // Assertion: The arrays must be completely identical, including all generated UUIDs
      expect(nodes1).toEqual(nodes2);

      // Sanity check to ensure IDs are actually populated and consistent
      expect(nodes1.length).toBeGreaterThan(0);
      nodes1.forEach((node, index) => {
        expect(node.id).toBeDefined();
        expect(node.id).toBe(nodes2[index].id);
        if ('turnId' in node) {
          expect(node.turnId).toBeDefined();
          expect(node.turnId).toBe((nodes2[index] as BaseConcreteNode).turnId);
        }
      });
    });
  });
});
