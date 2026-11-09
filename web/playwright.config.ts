import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./tests",
  workers: 1,
  fullyParallel: false,
  timeout: 30000,
  use: {
    baseURL: process.env.SIGBOT_WEB_URL ?? "http://127.0.0.1:15173",
    viewport: { width: 1440, height: 1100 },
    screenshot: "only-on-failure",
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE,
    },
    trace: "retain-on-failure",
    ...(process.env.SIGBOT_PLAYWRIGHT_STORAGE_STATE
      ? { storageState: process.env.SIGBOT_PLAYWRIGHT_STORAGE_STATE }
      : {}),
  },
  webServer: process.env.SIGBOT_WEB_URL ? undefined : {
    command: "npm run preview",
    url: "http://127.0.0.1:15173",
    reuseExistingServer: !process.env.CI,
    timeout: 30000,
  },
  reporter: [["list"], ["html", { open: "never" }]],
});
