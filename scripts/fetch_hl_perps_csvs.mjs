// scripts/fetch_hl_perps_csvs.mjs
import { chromium } from 'playwright';
import fs from 'fs/promises';
import path from 'path';

const OUT_DIR = process.env.OUT_DIR || 'data';
const VOLUME_CSV = path.join(OUT_DIR, 'hyperliquid_perps_volume.csv');
const OI_CSV     = path.join(OUT_DIR, 'hyperliquid_perps_open_interest.csv');

const PERPS_PAGE = 'https://defillama.com/perps';
const OI_PAGE    = 'https://defillama.com/open-interest';

// Many SPAs never hit "networkidle". We rely on "load" and explicit element waits.
async function downloadCsvFromPage(pageUrl, outPath, opts = {}) {
  const { headless = true, timeoutMs = 120_000 } = opts;

  const browser = await chromium.launch({ headless });
  const ctx = await browser.newContext({ acceptDownloads: true, userAgent:
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36'
  });
  const page = await ctx.newPage();

  // 1) Navigate and give the SPA some time to hydrate
  await page.goto(pageUrl, { waitUntil: 'load', timeout: timeoutMs });
  await page.waitForTimeout(4000); // small settle

  // 2) Switch to "Daily" granularity if present
  try {
    const dailyCandidates = [
      page.getByRole('button', { name: /^D$/ }),
      page.getByText(/^D$/),
      page.locator('button:has-text("D")')
    ];
    for (const cand of dailyCandidates) {
      if (await cand.count()) { await cand.first().click({ timeout: 2000 }).catch(()=>{}); break; }
    }
  } catch {}

  // 3) Find the Download button (multiple fallbacks)
  const btns = [
    page.getByRole('button', { name: /download\s*\.csv/i }),
    page.getByText(/download\s*\.csv/i),
    page.locator('button:has-text("Download .csv")'),
    page.locator('a:has-text("Download .csv")')
  ];

  let found = null;
  for (const b of btns) {
    try {
      await b.first().waitFor({ timeout: 30_000 });
      found = b.first();
      break;
    } catch {}
  }
  if (!found) throw new Error('Download .csv control not found');

  // 4) Click -> capture download
  const [download] = await Promise.all([
    page.waitForEvent('download', { timeout: timeoutMs }),
    found.click()
  ]);

  const tmp = await download.path();
  if (!tmp) throw new Error('No download path from Playwright');
  await fs.mkdir(path.dirname(outPath), { recursive: true });
  await fs.copyFile(tmp, outPath);
  console.log(`Saved CSV → ${outPath}`);

  await browser.close();
}

async function withRetry(fn, tries = 3, delayMs = 4000) {
  let last;
  for (let i = 1; i <= tries; i++) {
    try { return await fn(); }
    catch (e) {
      last = e;
      console.warn(`Attempt ${i}/${tries} failed: ${e?.message || e}`);
      if (i < tries) await new Promise(r => setTimeout(r, delayMs));
    }
  }
  throw last;
}

(async () => {
  await withRetry(() => downloadCsvFromPage(PERPS_PAGE, VOLUME_CSV));
  await withRetry(() => downloadCsvFromPage(OI_PAGE, OI_CSV));
})();