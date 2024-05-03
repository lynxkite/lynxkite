import type { PlaywrightTestConfig } from '@playwright/test';
import { devices } from '@playwright/test';

const config: PlaywrightTestConfig = {
  testDir: './tests',
  // Maximum time one test can run for.
  timeout: 60_000,
  expect: {
    // Maximum time expect() should wait for the condition to be met.
    timeout: process.env.CI ? 30_000 : 5_000,
  },
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: process.env.CI ? [['github'], ['html']] : 'html',
  use: {
    actionTimeout: 0,
    baseURL: process.env.LYNXKITE_ADDRESS || 'http://localhost:2200',
    trace: 'on',
  },
  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
      },
    },
  ],
};

export default config;
