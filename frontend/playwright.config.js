import { defineConfig } from "@playwright/test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

// Service databases/WAL must live on local temporary storage, independently of
// repository snapshots, artifacts and concurrent build/check processes.
process.env.DATAFLOW_CONSOLE_TEST_ROOT ||= mkdtempSync(path.join(tmpdir(), "dataflow-console-"));
export default defineConfig({
  testDir: "./tests",
  timeout: 60000,
  expect: { timeout: 15000 },
  workers: 1,
  fullyParallel: false,
  reporter: [["list"], ["html", { open: "never" }]],
  use: {
    baseURL: "http://127.0.0.1:8000",
    viewport: { width: 1440, height: 1000 },
    launchOptions: {
      executablePath: process.env.DATAFLOW_TEST_BROWSER || undefined,
    },
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  webServer: {
    command: `${process.env.DATAFLOW_TEST_PYTHON || "../.venv/bin/python"} tests/serve.py`,
    url: "http://127.0.0.1:8000",
    reuseExistingServer: false,
    timeout: 30000,
    env: { PYTHONPATH: "../src", DATAFLOW_CONSOLE_TEST_ROOT: process.env.DATAFLOW_CONSOLE_TEST_ROOT },
  },
});
