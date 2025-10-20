// scripts/fetch_hl_perps_csvs.mjs
import { chromium } from 'playwright';
import fs from 'fs/promises';
import path from 'path';

const OUT_DIR = process.env.OUT_DIR || 'data';
const VOLUME_CSV = path.join(OUT_DIR, 'hyperliquid_perps_volume.csv');
const OI_CSV     = path.join(OUT_DIR, 'hyperliquid_perps_open_interest.csv');

// Pages we’ll visit (public, keyless). We’ll select the protocol and click “Download .csv”.
const PERPS_PAGE = 'https://defillama.com/perps';
const OI_PAGE    = 'https://defillama.com/open-interest';

// Utility: download the visible CSV after searching for “Hyperliquid”
async function downloadCsv(pageUrl, searchBoxSelector, tableRowSelector, csvButtonText, outPath) {
  const browser = await chromium.launch();
  const ctx = await browser.newContext({ acceptDownloads: true });
  const page = await ctx.newPage();
  await page.goto(pageUrl, { waitUntil: 'domcontentloaded' });

  // Ensure page hydrates (UI is a SPA)
  await page.waitForTimeout(3000);

  // There’s a Search box near the table; type “Hyperliquid”
  const search = page.getByPlaceholder(/search/i).first().or(page.locator(searchBoxSelector));
  await search.fill('Hyperliquid');
  await page.keyboard.press('Enter');
  await page.waitForTimeout(1000);

  // We should see a filtered row; click to ensure the table is focused (optional).
  await page.locator(tableRowSelector).first().waitFor({ timeout: 15000 });

  // Ensure Daily granularity if a “D/W/M/C” toggle exists
  const dailyBtn = page.getByText(/^D$/).first();
  if (await dailyBtn.count()) {
    await dailyBtn.click();
    await page.waitForTimeout(1000);
  }

  // Click “Download .csv”
  const [download] = await Promise.all([
    page.waitForEvent('download', { timeout: 60000 }),
    page.getByText(csvButtonText, { exact: false }).click()
  ]);
  const tmp = await download.path();
  const buf = await fs.readFile(tmp);
  await fs.mkdir(path.dirname(outPath), { recursive: true });
  await fs.writeFile(outPath, buf);
  console.log(`Saved CSV → ${outPath}`);

  await browser.close();
}

(async () => {
  // 1) Volume CSV (Perps page)
  await downloadCsv(
    PERPS_PAGE,
    'input[type="search"]',
    'table tbody tr',
    'Download .csv',
    VOLUME_CSV
  );

  // 2) Open Interest CSV (Open Interest page)
  await downloadCsv(
    OI_PAGE,
    'input[type="search"]',
    'table tbody tr',
    'Download .csv',
    OI_CSV
  );
})();
