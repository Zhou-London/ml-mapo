import { LGraphCanvas, LiteGraph } from "@comfyorg/litegraph";

export type ThemeMode = "dark" | "light";
export type NodeCategory = "data" | "forecast" | "risk" | "opt" | "general";

export interface CategoryColors {
  color: string;
  bgcolor: string;
  boxcolor: string;
}

export interface ThemePalette {
  canvasBg: string;
  gridMinor: string;
  gridMajor: string;
  defaultLink: string;
  selectedLink: string;
  hoverLink: string;
  inputOff: string;
  inputOn: string;
  outputOff: string;
  outputOn: string;
  nodeTitle: string;
  nodeTitleSelected: string;
  nodeText: string;
  nodeSurface: string;
  nodeSurfaceLight: string;
  nodeOutline: string;
  widgetBg: string;
  observerHeader: string;
  observerBody: string;
  observerPlaceholder: string;
  nodeCategory: Record<NodeCategory, CategoryColors>;
}

/**
 * Per-port-type wire colors. Picked so each canonical edge type stays
 * distinguishable in both themes; unknown types fall back to a deterministic
 * hash-derived hue so newly-added schemas immediately get a stable color
 * without any extra wiring.
 */
const TYPE_COLORS_DARK: Record<string, string> = {
  ohlcv_snapshot: "#5fb3d8",
  covariance: "#f0a36b",
  alpha_series: "#9bd06b",
  alpha_scores: "#c2e069",
  weights: "#cd9bf0",
  dict: "#9aa6b8",
  date: "#e8c46a",
  Engine: "#7a93c4",
  int: "#8a96a6",
  str: "#8a96a6",
  float: "#8a96a6",
  bool: "#8a96a6",
};

const TYPE_COLORS_LIGHT: Record<string, string> = {
  ohlcv_snapshot: "#1d6fa8",
  covariance: "#b85a23",
  alpha_series: "#3f8a3f",
  alpha_scores: "#5a9b1f",
  weights: "#7d4ab4",
  dict: "#5a6779",
  date: "#a17320",
  Engine: "#3f5d8b",
  int: "#5b6776",
  str: "#5b6776",
  float: "#5b6776",
  bool: "#5b6776",
};

export const THEMES: Record<ThemeMode, ThemePalette> = {
  dark: {
    canvasBg: "#10161d",
    gridMinor: "rgba(145, 168, 189, 0.08)",
    gridMajor: "rgba(145, 168, 189, 0.18)",
    defaultLink: "#7d8696",
    selectedLink: "#ffd166",
    hoverLink: "#a4adbd",
    inputOff: "#7f5533",
    inputOn: "#ffb066",
    outputOff: "#36598c",
    outputOn: "#72a7ff",
    nodeTitle: "#ecf1f8",
    nodeTitleSelected: "#ffffff",
    nodeText: "#dde3ec",
    nodeSurface: "#27313b",
    nodeSurfaceLight: "#313d49",
    nodeOutline: "#5b6a7a",
    widgetBg: "#161d24",
    observerHeader: "rgba(255, 255, 255, 0.92)",
    observerBody: "rgba(228, 234, 244, 0.85)",
    observerPlaceholder: "rgba(180, 192, 210, 0.65)",
    nodeCategory: {
      data: { color: "#314b6a", bgcolor: "#3e618b", boxcolor: "#75a8e4" },
      forecast: { color: "#35563a", bgcolor: "#45724d", boxcolor: "#88d497" },
      risk: { color: "#5a3f2f", bgcolor: "#7b553d", boxcolor: "#e6a26f" },
      opt: { color: "#47395f", bgcolor: "#5b4a7d", boxcolor: "#bca0ef" },
      general: { color: "#33414f", bgcolor: "#465869", boxcolor: "#94a8bc" },
    },
  },
  light: {
    // Slightly tinted off-white for contrast against pure-white nodes.
    canvasBg: "#e7ecf2",
    gridMinor: "rgba(40, 64, 92, 0.10)",
    gridMajor: "rgba(40, 64, 92, 0.22)",
    defaultLink: "#5a6c80",
    selectedLink: "#d44c1c",
    hoverLink: "#2c3a4d",
    inputOff: "#a9621b",
    inputOn: "#d4640f",
    outputOff: "#4671b7",
    outputOn: "#1f4ea3",
    nodeTitle: "#f4f8fc",
    nodeTitleSelected: "#ffffff",
    // Nodes get medium-saturation backgrounds in light mode so the dark text
    // reads cleanly without needing a per-node alpha scrim.
    nodeText: "#1a2533",
    nodeSurface: "#f6f9fc",
    nodeSurfaceLight: "#dde6ef",
    nodeOutline: "#7d92ab",
    widgetBg: "#ffffff",
    observerHeader: "rgba(20, 32, 48, 0.92)",
    observerBody: "rgba(40, 56, 76, 0.86)",
    observerPlaceholder: "rgba(70, 88, 110, 0.7)",
    nodeCategory: {
      data: { color: "#1f4f8a", bgcolor: "#5d92d3", boxcolor: "#1f4f8a" },
      forecast: { color: "#2b6b3a", bgcolor: "#65b06f", boxcolor: "#2b6b3a" },
      risk: { color: "#8a4a25", bgcolor: "#d28954", boxcolor: "#8a4a25" },
      opt: { color: "#5a3a8a", bgcolor: "#9678c3", boxcolor: "#5a3a8a" },
      general: { color: "#3d4a5a", bgcolor: "#7c8ea4", boxcolor: "#3d4a5a" },
    },
  },
};

export function categoryFromType(type: string): NodeCategory {
  const prefix = type.split("/")[0];
  switch (prefix) {
    case "data":
      return "data";
    case "forecast":
      return "forecast";
    case "risk":
      return "risk";
    case "opt":
    case "optimization":
      return "opt";
    default:
      return "general";
  }
}

/**
 * Resolve a per-port-type color. Known canonical types use the curated
 * palette; everything else gets a deterministic HSL color from a simple
 * string hash so adding a new port type doesn't require palette changes.
 */
export function colorForPortType(type: string, theme: ThemeMode): string {
  const palette = theme === "dark" ? TYPE_COLORS_DARK : TYPE_COLORS_LIGHT;
  if (type in palette) return palette[type];
  const hue = hashHue(type);
  return theme === "dark"
    ? `hsl(${hue}, 60%, 65%)`
    : `hsl(${hue}, 55%, 38%)`;
}

function hashHue(value: string): number {
  let h = 0;
  for (let i = 0; i < value.length; i++) {
    h = (h * 31 + value.charCodeAt(i)) | 0;
  }
  return Math.abs(h) % 360;
}

/**
 * Push the palette onto the LiteGraph globals + the canvas. Called whenever
 * the theme changes or new schemas arrive (so per-type wire colors get
 * registered for any newly-introduced edge types).
 */
export function applyCanvasTheme(
  canvas: LGraphCanvas,
  theme: ThemeMode,
  knownPortTypes: Iterable<string>,
): void {
  const palette = THEMES[theme];

  canvas.clear_background_color = palette.canvasBg;
  canvas.default_link_color = palette.defaultLink;
  canvas.default_connection_color = {
    input_off: palette.inputOff,
    input_on: palette.inputOn,
    output_off: palette.outputOff,
    output_on: palette.outputOn,
  };

  const byType: Record<string, string> = {};
  for (const type of knownPortTypes) {
    if (!type) continue;
    byType[type] = colorForPortType(type, theme);
  }
  canvas.default_connection_color_byType = byType;
  canvas.default_connection_color_byTypeOff = byType;
  LGraphCanvas.link_type_colors = { ...LGraphCanvas.link_type_colors, ...byType };

  // Selection highlight — different LiteGraph builds expose this under
  // different names; set them all so whichever the renderer reads picks up
  // the high-contrast value.
  const lgCanvasAny = LGraphCanvas as unknown as Record<string, string>;
  lgCanvasAny.link_color_selected = palette.selectedLink;
  lgCanvasAny.link_color_highlighted = palette.selectedLink;
  const canvasAny = canvas as unknown as Record<string, string | number>;
  canvasAny.highlighted_link_color = palette.selectedLink;
  canvasAny.connecting_link_color = palette.selectedLink;
  canvasAny.connections_width = 2.6;
  canvasAny.connections_shadow = 0;

  LiteGraph.NODE_TITLE_COLOR = palette.nodeTitle;
  LiteGraph.NODE_SELECTED_TITLE_COLOR = palette.nodeTitleSelected;
  LiteGraph.NODE_TEXT_COLOR = palette.nodeText;
  LiteGraph.NODE_DEFAULT_COLOR = palette.nodeSurfaceLight;
  LiteGraph.NODE_DEFAULT_BGCOLOR = palette.nodeSurface;
  LiteGraph.NODE_DEFAULT_BOXCOLOR = palette.nodeOutline;
  LiteGraph.NODE_BOX_OUTLINE_COLOR = palette.nodeOutline;
  LiteGraph.WIDGET_BGCOLOR = palette.widgetBg;
  LiteGraph.WIDGET_TEXT_COLOR = palette.nodeText;
  // Newer @comfyorg/litegraph versions expose a few extra constants for
  // selected/active link highlights — set them defensively.
  const liteAny = LiteGraph as unknown as Record<string, string>;
  liteAny.LINK_COLOR = palette.defaultLink;
  liteAny.NODE_SELECTED_COLOR = palette.selectedLink;
}

export function applyGraphTheme(
  graph: import("@comfyorg/litegraph").LGraph,
  theme: ThemeMode,
): void {
  const palette = THEMES[theme];
  const nodes = graph._nodes ?? [];
  for (const rawNode of nodes) {
    const node = rawNode as import("@comfyorg/litegraph").LGraphNode & {
      color?: string;
      bgcolor?: string;
      boxcolor?: string;
    };
    const category = categoryFromType(node.type as string);
    const colors = palette.nodeCategory[category];
    node.color = colors.color;
    node.bgcolor = colors.bgcolor;
    node.boxcolor = colors.boxcolor;
  }

  const links = graph.links;
  if (!links) return;
  const values =
    typeof (links as { values?: () => Iterable<unknown> }).values === "function"
      ? (links as { values: () => Iterable<unknown> }).values()
      : (Object.values(links as unknown as Record<string, unknown>) as Iterable<unknown>);
  for (const rawLink of values) {
    if (!rawLink || typeof rawLink !== "object") continue;
    const link = rawLink as { type?: string; color?: string };
    link.color = link.type ? colorForPortType(link.type, theme) : palette.defaultLink;
  }
}
