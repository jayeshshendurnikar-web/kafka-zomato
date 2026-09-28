import test from 'node:test';
import assert from 'node:assert/strict';
import { chromium } from 'playwright';

test(
  'two React rider sessions reach their own customers and lock active rider/order fields',
  { timeout: 60000 },
  async () => {
    const browser = await chromium.launch({ headless: true });
    const url = process.env.APP_URL || 'http://127.0.0.1:5173';
    const sessions = [];
    const errors = [];
    try {
      for (let index = 1; index <= 2; index++) {
        const page = await browser.newPage();
        page.on('pageerror', (error) => errors.push(error.message));
        const orderId = `browser-${Date.now()}-${index}`;
        const riderId = `RIDER-0${index}`;
        sessions.push({ page, orderId, riderId });
        await page.goto(url);
        await page.locator('#customer-order').fill(orderId);
        await page.getByRole('button', { name: 'Track my delivery' }).click();
        await page.getByRole('tab', { name: 'Rider', exact: true }).click();
        await page.locator('#rider-order').fill(orderId);
        await page.locator('#rider-id').fill(riderId);
        await page.getByLabel('Location source').selectOption('demo');
        await page.getByRole('button', { name: 'Start sharing' }).click();
        await page.waitForFunction(
          (id) => document.querySelector('.rider-card strong')?.textContent === id,
          riderId,
        );
        assert.equal(await page.locator('#rider-id').isDisabled(), true);
        assert.equal(await page.locator('#rider-order').isDisabled(), true);
      }
      for (const { page, orderId, riderId } of sessions) {
        const timestamp = await page.locator('.metrics strong').last().textContent();
        await page.waitForFunction(
          (previous) =>
            document.querySelector('.metrics > div:last-child strong')?.textContent !== previous,
          timestamp,
        );
        assert.equal(await page.locator('.map-header h2').textContent(), orderId);
        assert.equal(await page.locator('.rider-card strong').textContent(), riderId);
        await page.getByRole('button', { name: 'Stop sharing' }).click();
        assert.equal(await page.locator('#rider-id').isDisabled(), false);
        await page.getByRole('tab', { name: 'Customer', exact: true }).click();
        await page.getByRole('button', { name: 'Disconnect', exact: true }).click();
      }
      assert.deepEqual(errors, []);
    } finally {
      await browser.close();
    }
  },
);
