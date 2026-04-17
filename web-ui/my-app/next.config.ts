import type { NextConfig } from "next";

const config: NextConfig = {
  // Our Playwright tests (and some LAN setups) hit the dev server by its
  // 127.0.0.1 alias rather than "localhost". Next 16 treats that as a
  // cross-origin dev request by default and blocks HMR/asset fetches.
  allowedDevOrigins: ["127.0.0.1", "localhost"],
};

export default config;
