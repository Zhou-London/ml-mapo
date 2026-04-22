import type { NextConfig } from "next";

const config: NextConfig = {
  // Our Playwright tests (and some LAN setups) hit the dev server by its
  // 127.0.0.1 alias rather than "localhost". Next 16 treats that as a
  // cross-origin dev request by default and blocks HMR/asset fetches.
  allowedDevOrigins: ["127.0.0.1", "localhost"],
  // The floating "N" dev-mode badge sits in the bottom-left and overlaps
  // the palette hint in the graph editor — hide it.
  devIndicators: false,
  // pdoc generates a static site under public/docs/. Next doesn't auto-serve
  // directory indexes, so /docs and /docs/ need to map to the index file.
  async rewrites() {
    return [
      { source: "/docs", destination: "/docs/index.html" },
      { source: "/docs/", destination: "/docs/index.html" },
    ];
  },
};

export default config;
