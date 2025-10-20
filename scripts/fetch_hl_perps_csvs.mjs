// scripts/fetch_hl_perps_csvs.mjs
import { chromium } from 'playwright';
import fs from 'fs/promises';
import path from 'path';

const OUT_DIR = process.env.OUT_DIR || 'data';
const VOLUME_CSV = path.join(OUT_DIR, 'hyperliquid_perps_volume.csv');
const OI_CSV     = path.join(OUT_DIR, 'hyperliquid_perps_open_interest.csv');

const PERPS_PAGE = 'https://defillama.com/perps';
const OI_PAGE    = 'https://defillama.com/open-interest';

async function downloadCsvFromPage(pageUrl, outPath, opts = {}) {
  const { waitAfterLoadMs = 4000, timeoutMs = 90000, headless = true } = opts;
  const browser = await chromium.launch({ headless });
  const ctx = await browser.newContext({ acceptDownloads: true });
  const page = await ctx.newPage();

  // Go to page and let the SPA hydrate
  await page.goto(pageUrl, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
  await page.waitForLoadState('networkidle', { timeout: timeoutMs });
  await page.waitForTimeout(waitAfterLoadMs);

  // If a D/W/M/C toggle exists, select Daily (D)
  const dailyBtn = page.getByText(/^D$/).first();
  if (await dailyBtn.count()) {
    await dailyBtn.click().catch(() => {});
    await page.waitForTimeout(1000);
  }

  // Wait for “Download .csv” button (rendered by JS), then click and save the file
  const dlBtn = page.getByText('Download .csv', { exact: false });
  await dlBtn.waitFor({ timeout: timeoutMs });

  const [download] = await Promise.all([
    page.waitForEvent('download', { timeout: timeoutMs }),
    dlBtn.click()
  ]);

  const tmpPath = await download.path();
  if (!tmpPath) throw new Error('No download path returned.');
  const buf = await fs.readFile(tmpPath);
  await fs.mkdir(path.dirname(outPath), { recursive: true });
  await fs.writeFile(outPath, buf);
  console.log(`Saved CSV → ${outPath}`);

  await browser.close();
}

async function withRetry(fn, tries = 3, delayMs = 3000) {
  let lastErr;
  for (let i = 1; i <= tries; i++) {
    try { return await fn(); }
    catch (e) {
      lastErr = e;
      console.warn(`Attempt ${i}/${tries} failed: ${e?.message || e}`);
      if (i < tries) await new Promise(r => setTimeout(r, delayMs));
    }
  }
  throw lastErr;
}

(async () => {
  // 1) Perps (volume CSV)
  await withRetry(() => downloadCsvFromPage(PERPS_PAGE, VOLUME_CSV));

  // 2) Open Interest CSV
  await withRetry(() => downloadCsvFromPage(OI_PAGE, OI_CSV));
})();