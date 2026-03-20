import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/e2e",
  timeout: 30_000,
  retries: 1,
  use: {
    baseURL: "http://localhost:5173/ml-toolbox/",
    headless: true,
  },
  webServer: {
    command: "pnpm dev",
    url: "http://localhost:5173/ml-toolbox/",
    reuseExistingServer: !process.env.CI,
    timeout: 15_000,
  },
});
