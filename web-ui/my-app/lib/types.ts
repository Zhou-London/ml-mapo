export interface SchemaPort {
  name: string;
  type: string;
}

export interface SchemaParam {
  name: string;
  type: string;
  default?: unknown;
}

export interface NodeSchema {
  type: string;
  category: string;
  doc?: string;
  inputs: SchemaPort[];
  outputs: SchemaPort[];
  params: SchemaParam[];
}

export interface GraphNode {
  id: string;
  type: string;
  params?: Record<string, unknown>;
  pos?: [number, number];
  size?: [number, number];
  disabled?: boolean;
}

export interface GraphEdge {
  src_node: string;
  src_port: string;
  dst_node: string;
  dst_port: string;
}

export interface GraphDoc {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface RunResult {
  ok: boolean;
  code: number;
  stdout: string;
  stderr: string;
}
