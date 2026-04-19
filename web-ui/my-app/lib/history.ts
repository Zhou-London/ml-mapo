import type { GraphDoc } from "./types";

/**
 * Linear undo/redo stack for serialized graph documents.
 *
 * The editor snapshots the whole graph after each user-visible mutation
 * (debounced by the caller). Storing full GraphDoc payloads keeps restoration
 * trivial — just rehydrate via loadIntoGraph — at the cost of memory per
 * entry. We cap the depth so a long editing session can't grow unbounded.
 */
export class HistoryStack {
  private stack: GraphDoc[] = [];
  private cursor = -1;
  private readonly cap: number;

  constructor(cap = 100) {
    this.cap = cap;
  }

  reset(initial: GraphDoc): void {
    this.stack = [clone(initial)];
    this.cursor = 0;
  }

  /** Push a snapshot, dropping any redoable future and trimming oldest entries. */
  push(doc: GraphDoc): void {
    if (this.cursor >= 0 && sameDoc(this.stack[this.cursor], doc)) return;
    this.stack.length = this.cursor + 1;
    this.stack.push(clone(doc));
    if (this.stack.length > this.cap) {
      const drop = this.stack.length - this.cap;
      this.stack.splice(0, drop);
    }
    this.cursor = this.stack.length - 1;
  }

  canUndo(): boolean {
    return this.cursor > 0;
  }

  canRedo(): boolean {
    return this.cursor >= 0 && this.cursor < this.stack.length - 1;
  }

  undo(): GraphDoc | null {
    if (!this.canUndo()) return null;
    this.cursor -= 1;
    return clone(this.stack[this.cursor]);
  }

  redo(): GraphDoc | null {
    if (!this.canRedo()) return null;
    this.cursor += 1;
    return clone(this.stack[this.cursor]);
  }

  current(): GraphDoc | null {
    return this.cursor >= 0 ? clone(this.stack[this.cursor]) : null;
  }
}

function clone(doc: GraphDoc): GraphDoc {
  // structuredClone is available everywhere we run (Node 22, modern browsers).
  return structuredClone(doc);
}

function sameDoc(a: GraphDoc, b: GraphDoc): boolean {
  if (a.nodes.length !== b.nodes.length) return false;
  if (a.edges.length !== b.edges.length) return false;
  // Cheap structural equality — JSON.stringify is fast enough for the small
  // graphs we deal with and avoids deep-equality utility deps.
  return JSON.stringify(a) === JSON.stringify(b);
}
