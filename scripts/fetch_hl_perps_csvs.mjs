import { chromium } from 'playwright';
import fs from 'fs/promises';
import path from 'path';

const VOLUME_CSV = process.env.VOLUME_CSV || '/tmp/hyperliquid_perps_volume.csv';
const OI_CSV     = process.env.OI_CSV     || '/tmp/hyperliquid_perps_open_interest.csv';

const PERPS_PAGE = 'https://defillama.com/perps';
const OI_PAGE    = 'https://defillama.com/open-interest';

async function downloadCsvFromPage(pageUrl, outPath, timeoutMs = 120_000) {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ acceptDownloads: true });
  const page = await ctx.newPage();

  await page.goto(pageUrl, { waitUntil: 'load', timeout: timeoutMs });
  await page.waitForTimeout(4000);

  // Select "Daily" if present; ignore failures
  try {
    const daily = page.getByRole('button', { name: /^D$/ }).first();
    if (await daily.count()) await daily.click();
  } catch {}

  // Find & click “Download .csv”
  const candidates = [
    page.getByRole('button', { name: /download\s*\.csv/i }),
    page.getByText(/download\s*\.csv/i),
    page.locator('button:has-text("Download .csv")'),
    page.locator('a:has-text("Download .csv")'),
  ];
  let btn = null;
  for (const c of candidates) {
    try { await c.first().waitFor({ timeout: 30000 }); btn = c.first(); break; } catch {}
  }
  if (!btn) throw new Error('Download .csv control not found');

  const [download] = await Promise.all([
    page.waitForEvent('download', { timeout: timeoutMs }),
    btn.click()
  ]);

  const tmp = await download.path();
  if (!tmp) throw new Error('No download path from Playwright');
  await fs.copyFile(tmp, outPath);
  console.log(`Saved CSV → ${outPath}`);

  await browser.close();
}

(async () => {
  await downloadCsvFromPage(PERPS_PAGE, VOLUME_CSV);
  await downloadCsvFromPage(OI_PAGE, OI_CSV);
})();