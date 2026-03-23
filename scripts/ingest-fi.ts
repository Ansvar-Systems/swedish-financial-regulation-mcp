/**
 * Finansinspektionen (FI) Ingestion Crawler
 *
 * Scrapes fi.se to populate the SQLite database with:
 *   - FFFS regulations (foreskrifter och allmanna rad)
 *   - Enforcement actions (sanctions against financial firms and market abuse)
 *   - FI promemorior and tillsynsrapporter from the publications feed
 *
 * Data sources:
 *   - https://www.fi.se/en/our-registers/fffs/list-of-regulations/
 *   - https://www.fi.se/en/our-registers/fffs/search-fffs/{year}/{ref}/
 *   - https://www.fi.se/en/published/sanctions/financial-firms/
 *   - https://www.fi.se/en/published/sanctions/financial-reporting-supervision/
 *   - https://www.fi.se/en/published/ (general publications)
 *
 * Usage:
 *   npx tsx scripts/ingest-fi.ts                    # full crawl
 *   npx tsx scripts/ingest-fi.ts --resume           # resume from last checkpoint
 *   npx tsx scripts/ingest-fi.ts --dry-run          # fetch + parse without DB writes
 *   npx tsx scripts/ingest-fi.ts --force            # drop DB and re-crawl from scratch
 *   npx tsx scripts/ingest-fi.ts --fffs-only        # only crawl FFFS regulations
 *   npx tsx scripts/ingest-fi.ts --sanctions-only   # only crawl sanctions
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, readFileSync, writeFileSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";
import * as cheerio from "cheerio";

// ── Configuration ───────────────────────────────────────────────────────────

const BASE_URL = "https://www.fi.se";
const DB_PATH = process.env["FI_SE_DB_PATH"] ?? "data/fi-se.db";
const PROGRESS_PATH = process.env["FI_PROGRESS_PATH"] ?? "data/ingest-progress.json";
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 2000;
const REQUEST_TIMEOUT_MS = 30_000;

const USER_AGENT =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) " +
  "Chrome/120.0.0.0 Safari/537.36 AnsvarMCP/1.0 (+https://ansvar.eu)";

// ── CLI argument parsing ────────────────────────────────────────────────────

interface CliArgs {
  resume: boolean;
  dryRun: boolean;
  force: boolean;
  fffsOnly: boolean;
  sanctionsOnly: boolean;
}

function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  const result: CliArgs = {
    resume: false,
    dryRun: false,
    force: false,
    fffsOnly: false,
    sanctionsOnly: false,
  };

  for (const arg of args) {
    switch (arg) {
      case "--resume":
        result.resume = true;
        break;
      case "--dry-run":
        result.dryRun = true;
        break;
      case "--force":
        result.force = true;
        break;
      case "--fffs-only":
        result.fffsOnly = true;
        break;
      case "--sanctions-only":
        result.sanctionsOnly = true;
        break;
      default:
        console.error(`Unknown argument: ${arg}`);
        console.error(
          "Usage: npx tsx scripts/ingest-fi.ts [--resume] [--dry-run] [--force] [--fffs-only] [--sanctions-only]",
        );
        process.exit(1);
    }
  }

  if (result.fffsOnly && result.sanctionsOnly) {
    console.error("Cannot use both --fffs-only and --sanctions-only");
    process.exit(1);
  }

  return result;
}

// ── Progress tracking ───────────────────────────────────────────────────────

interface Progress {
  completed_fffs_pages: string[];
  completed_fffs_detail: string[];
  completed_sanction_categories: string[];
  completed_sanction_pages: string[];
  last_updated: string;
}

function loadProgress(): Progress {
  if (existsSync(PROGRESS_PATH)) {
    try {
      return JSON.parse(readFileSync(PROGRESS_PATH, "utf-8")) as Progress;
    } catch {
      // Corrupted file, start fresh
    }
  }
  return {
    completed_fffs_pages: [],
    completed_fffs_detail: [],
    completed_sanction_categories: [],
    completed_sanction_pages: [],
    last_updated: new Date().toISOString(),
  };
}

function saveProgress(progress: Progress): void {
  const dir = dirname(PROGRESS_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  progress.last_updated = new Date().toISOString();
  writeFileSync(PROGRESS_PATH, JSON.stringify(progress, null, 2));
}

// ── HTTP utilities ──────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }
  lastRequestTime = Date.now();

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "en-US,en;q=0.9,sv;q=0.8",
        },
        signal: controller.signal,
      });

      clearTimeout(timeout);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status} ${response.statusText}`);
      }

      return await response.text();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(
        `  [attempt ${attempt}/${MAX_RETRIES}] ${url}: ${msg}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_BACKOFF_MS * attempt);
      } else {
        throw new Error(`Failed after ${MAX_RETRIES} attempts: ${url} — ${msg}`);
      }
    }
  }

  // Unreachable, but TypeScript needs it
  throw new Error(`Failed to fetch ${url}`);
}

async function fetchPdfText(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }
  lastRequestTime = Date.now();

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS * 2);

    const response = await fetch(url, {
      headers: { "User-Agent": USER_AGENT },
      signal: controller.signal,
    });

    clearTimeout(timeout);

    if (!response.ok) {
      console.warn(`  PDF fetch failed: HTTP ${response.status} for ${url}`);
      return null;
    }

    // We cannot parse PDFs natively in Node without a library.
    // Return null and rely on the HTML page metadata instead.
    return null;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(`  PDF fetch error: ${msg}`);
    return null;
  }
}

// ── Database operations ─────────────────────────────────────────────────────

function initDb(force: boolean): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  return db;
}

function upsertSourcebook(
  db: Database.Database,
  id: string,
  name: string,
  description: string,
): void {
  db.prepare(
    "INSERT OR REPLACE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
  ).run(id, name, description);
}

function insertProvision(
  db: Database.Database,
  provision: {
    sourcebook_id: string;
    reference: string;
    title: string | null;
    text: string;
    type: string | null;
    status: string;
    effective_date: string | null;
    chapter: string | null;
    section: string | null;
  },
): boolean {
  const result = db
    .prepare(
      `INSERT OR IGNORE INTO provisions
       (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    )
    .run(
      provision.sourcebook_id,
      provision.reference,
      provision.title,
      provision.text,
      provision.type,
      provision.status,
      provision.effective_date,
      provision.chapter,
      provision.section,
    );
  return result.changes > 0;
}

function insertEnforcement(
  db: Database.Database,
  action: {
    firm_name: string;
    reference_number: string | null;
    action_type: string | null;
    amount: number | null;
    date: string | null;
    summary: string | null;
    sourcebook_references: string | null;
  },
): boolean {
  const result = db
    .prepare(
      `INSERT OR IGNORE INTO enforcement_actions
       (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
    )
    .run(
      action.firm_name,
      action.reference_number,
      action.action_type,
      action.amount,
      action.date,
      action.summary,
      action.sourcebook_references,
    );
  return result.changes > 0;
}

// ── FFFS regulation list parsing ────────────────────────────────────────────

interface FffsListEntry {
  number: string;       // e.g. "2014:1"
  title: string;
  type: string;         // "basic" or "amendment"
  detailUrl: string;    // relative URL to detail page
  baseRegulation: string | null; // for amendments, the base regulation number
}

function parseFffsListPage(html: string): FffsListEntry[] {
  const $ = cheerio.load(html);
  const entries: FffsListEntry[] = [];

  // The list page uses <dl> elements with <dt>/<dd> pairs.
  // Each regulation has three pairs: Number, Heading, Type.
  // We scan for all <dd> elements and group them by threes.
  const allDt = $("dt");
  const allDd = $("dd");

  // Group entries: every 3 dt/dd pairs form one regulation
  const count = Math.floor(allDt.length / 3);

  for (let i = 0; i < count; i++) {
    const numberDd = allDd.eq(i * 3);
    const headingDd = allDd.eq(i * 3 + 1);
    const typeDd = allDd.eq(i * 3 + 2);

    const number = numberDd.text().trim();
    const headingLink = headingDd.find("a");
    const title = headingLink.text().trim() || headingDd.text().trim();
    const href = headingLink.attr("href") ?? "";
    const typeText = typeDd.text().trim().toLowerCase();

    if (!number || !title) continue;

    const isAmendment = typeText.includes("amendment") || typeText.includes("amending") || typeText.includes("andring");
    const baseMatch = typeText.match(/(?:basic regulation|grundforeskrift)\s+(?:FFFS\s+)?(\d{4}:\d+)/i);

    entries.push({
      number,
      title,
      type: isAmendment ? "amendment" : "basic",
      detailUrl: href.startsWith("http") ? href : href,
      baseRegulation: baseMatch?.[1] ?? null,
    });
  }

  // Fallback: if dl-based parsing yields nothing, try link-based scanning
  if (entries.length === 0) {
    $("a[href]").each((_i, el) => {
      const href = $(el).attr("href") ?? "";
      const text = $(el).text().trim();

      // Match FFFS detail page URLs: /en/our-registers/fffs/search-fffs/YEAR/YEARNUM/
      const match = href.match(/\/fffs\/search-fffs\/(\d{4})\/(\d{4,})(?:\/(\d+))?/);
      if (match && text.length > 10) {
        const year = match[1]!;
        const refNum = match[2]!;
        // Reconstruct the FFFS number from the URL
        const fffsNumber = refNum.length > 4
          ? `${year}:${refNum.slice(4)}`
          : `${year}:${refNum}`;

        entries.push({
          number: fffsNumber,
          title: text,
          type: match[3] ? "amendment" : "basic",
          detailUrl: href,
          baseRegulation: null,
        });
      }
    });
  }

  return entries;
}

// ── FFFS detail page parsing ────────────────────────────────────────────────

interface FffsDetail {
  title: string;
  summary: string;
  effectiveDate: string | null;
  status: string;
  amendments: Array<{
    number: string;
    date: string | null;
  }>;
  pdfUrls: string[];
}

function parseFffsDetailPage(html: string): FffsDetail {
  const $ = cheerio.load(html);

  // Extract the main title from h1 or h2
  const title =
    $("h1").first().text().trim() ||
    $("h2").first().text().trim() ||
    "";

  // Extract summary from the main content area
  const contentArea = $(".editor-content, .collection-main-body, main, article").first();
  const paragraphs: string[] = [];
  contentArea.find("p").each((_i, el) => {
    const text = $(el).text().trim();
    if (text.length > 20) {
      paragraphs.push(text);
    }
  });
  const summary = paragraphs.join("\n\n");

  // Extract effective date — look for patterns like "In force from YYYY-MM-DD"
  // or "Trader i kraft YYYY-MM-DD"
  let effectiveDate: string | null = null;
  const datePatterns = [
    /(?:in\s+force\s+from|trader\s+i\s+kraft|ikrafttrader|effective)\s+(\d{4}-\d{2}-\d{2})/i,
    /(\d{4}-\d{2}-\d{2})/,
  ];

  const fullText = contentArea.text();
  for (const pattern of datePatterns) {
    const match = fullText.match(pattern);
    if (match?.[1]) {
      effectiveDate = match[1];
      break;
    }
  }

  // Determine status
  const textLower = fullText.toLowerCase();
  let status = "in_force";
  if (textLower.includes("repealed") || textLower.includes("upphavd") || textLower.includes("upphavda")) {
    status = "deleted";
  } else if (textLower.includes("not yet in force") || textLower.includes("ej ikrafttradd")) {
    status = "not_yet_in_force";
  }

  // Extract amendment history
  const amendments: Array<{ number: string; date: string | null }> = [];
  const amendmentPattern = /(?:FFFS\s+)?(\d{4}:\d+)\s*(?:.*?(\d{4}-\d{2}-\d{2}))?/g;
  let amMatch: RegExpExecArray | null;
  while ((amMatch = amendmentPattern.exec(fullText)) !== null) {
    if (amMatch[1] && !amendments.some((a) => a.number === amMatch![1])) {
      amendments.push({
        number: amMatch[1],
        date: amMatch[2] ?? null,
      });
    }
  }

  // Extract PDF links
  const pdfUrls: string[] = [];
  $('a[href$=".pdf"]').each((_i, el) => {
    const href = $(el).attr("href");
    if (href) {
      const fullUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
      pdfUrls.push(fullUrl);
    }
  });

  return {
    title,
    summary,
    effectiveDate,
    status,
    amendments,
    pdfUrls,
  };
}

// ── Sanctions page parsing ──────────────────────────────────────────────────

interface SanctionListEntry {
  firmName: string;
  date: string | null;
  description: string;
  detailUrl: string;
  categories: string[];
}

function parseSanctionsListPage(html: string): {
  entries: SanctionListEntry[];
  nextPageUrl: string | null;
} {
  const $ = cheerio.load(html);
  const entries: SanctionListEntry[] = [];

  // Sanctions are listed as h3 headings with links, followed by date/category
  // and a description paragraph.
  // Structure: <h3><a href="...">Firm gets sanction</a></h3>
  //            <p>2024-12-11 | Tag1, Tag2</p>
  //            <p>Description text</p>

  // Strategy 1: Look for article/section blocks with heading links
  const headings = $("h3 a[href], h2 a[href]");

  headings.each((_i, el) => {
    const $link = $(el);
    const href = $link.attr("href") ?? "";
    const headingText = $link.text().trim();

    // Only process sanction-related links
    if (!href.includes("/sanctions/") && !href.includes("/sanktioner/")) {
      return;
    }

    // Navigate to the parent heading and find sibling content
    const $heading = $link.closest("h3, h2");
    const siblingTexts: string[] = [];

    let next = $heading.next();
    while (next.length > 0 && !next.is("h3, h2")) {
      siblingTexts.push(next.text().trim());
      next = next.next();
    }

    // Extract date from the first sibling
    let date: string | null = null;
    const categories: string[] = [];
    let description = "";

    for (const sibText of siblingTexts) {

      // Date pattern: YYYY-MM-DD
      const dateMatch = sibText.match(/(\d{4}-\d{2}-\d{2})/);
      if (dateMatch?.[1] && !date) {
        date = dateMatch[1];

        // Categories are often separated by | after the date
        const afterDate = sibText.slice(sibText.indexOf(date) + 10).trim();
        if (afterDate.startsWith("|")) {
          const tagText = afterDate.slice(1).trim();
          categories.push(
            ...tagText.split(/[,|]/).map((t) => t.trim()).filter(Boolean),
          );
        }
      } else if (sibText.length > 20 && !description) {
        description = sibText;
      }
    }

    // Extract firm name from heading text
    // Headings are like "Klarna far en anmarkning..." or "FI sanctions SBB..."
    const firmName = extractFirmNameFromHeading(headingText);

    if (firmName) {
      entries.push({
        firmName,
        date,
        description: description || headingText,
        detailUrl: href.startsWith("http") ? href : href,
        categories,
      });
    }
  });

  // Fallback: if heading-based parsing yields nothing, look for list items
  if (entries.length === 0) {
    $("a[href]").each((_i, el) => {
      const $link = $(el);
      const href = $link.attr("href") ?? "";
      const text = $link.text().trim();

      if (
        (href.includes("/sanctions/financial-firms/") ||
          href.includes("/sanktioner/finansiella-foretag/") ||
          href.includes("/sanctions/financial-reporting-supervision/") ||
          href.includes("/sanktioner/marknadsmissbruk/")) &&
        href.match(/\/\d{4}\//) &&
        text.length > 10
      ) {
        const firmName = extractFirmNameFromHeading(text);
        const dateMatch = href.match(/\/(\d{4})\//);

        if (firmName) {
          entries.push({
            firmName,
            date: dateMatch?.[1] ? `${dateMatch[1]}-01-01` : null,
            description: text,
            detailUrl: href,
            categories: [],
          });
        }
      }
    });
  }

  // Check for "next page" / "Visa fler" link
  let nextPageUrl: string | null = null;
  $("a").each((_i, el) => {
    const href = $(el).attr("href") ?? "";
    const text = $(el).text().trim().toLowerCase();
    if (
      (text.includes("visa fler") || text.includes("show more") || text.includes("next")) &&
      href.includes("page=")
    ) {
      nextPageUrl = href.startsWith("http") ? href : href;
    }
  });

  return { entries, nextPageUrl };
}

/**
 * Extract a firm name from a sanction heading.
 *
 * Headings follow patterns like:
 *   "Klarna far en anmarkning och sanktionsavgift"
 *   "FI ger Alecta en varning med sanktionsavgift"
 *   "FI withdraws authorisation for Intergiro"
 *   "Swedbank receives an administrative fine"
 *   "Fysisk person far sanktionsavgift for marknadsmanipulation i Dicot Pharma AB"
 */
function extractFirmNameFromHeading(heading: string): string | null {
  // Try Swedish patterns first
  const svPatterns = [
    // "FI ger X en varning..."
    /FI\s+(?:ger|alagen?r)\s+(.+?)\s+(?:en\s+)?(?:varning|anmarkning|sanktionsavgift)/i,
    // "X far en anmarkning..."
    /^(.+?)\s+far\s+(?:en\s+)?(?:varning|anmarkning|sanktionsavgift)/i,
    // "FI aterkallar tillstandet for X"
    /FI\s+(?:aterkallar|drar\s+in)\s+.*?(?:for|fran)\s+(.+?)$/i,
    // "Fysisk person far sanktionsavgift for ... i X AB"
    /sanktionsavgift\s+for\s+.*?\s+i\s+(.+?)$/i,
    // "Fysisk person doms att betala ... i X AB"
    /doms\s+att\s+betala\s+.*?\s+i\s+(.+?)$/i,
  ];

  for (const pattern of svPatterns) {
    const match = heading.match(pattern);
    if (match?.[1]) {
      return match[1].trim();
    }
  }

  // Try English patterns
  const enPatterns = [
    // "FI sanctions X for..."
    /FI\s+sanctions\s+(.+?)\s+for/i,
    // "X receives a/an ..."
    /^(.+?)\s+receives?\s+(?:a|an)\s+/i,
    // "FI withdraws authorisation for X"
    /FI\s+withdraws?\s+.*?(?:for|from)\s+(.+?)$/i,
    // "FI issues X a warning..."
    /FI\s+issues?\s+(.+?)\s+(?:a\s+)?(?:warning|remark|fine)/i,
  ];

  for (const pattern of enPatterns) {
    const match = heading.match(pattern);
    if (match?.[1]) {
      return match[1].trim();
    }
  }

  // Last resort: use the full heading trimmed and cleaned
  const cleaned = heading
    .replace(/^FI\s+/i, "")
    .replace(/\s+far\s+.*$/i, "")
    .replace(/\s+receives?\s+.*$/i, "")
    .trim();

  return cleaned.length > 2 && cleaned.length < 200 ? cleaned : null;
}

// ── Sanction detail page parsing ────────────────────────────────────────────

interface SanctionDetail {
  firmName: string;
  date: string | null;
  actionType: string | null;
  amount: number | null;
  summary: string;
  referenceNumber: string | null;
  sourcebookReferences: string | null;
}

function parseSanctionDetailPage(html: string, fallbackFirmName: string): SanctionDetail {
  const $ = cheerio.load(html);

  const contentArea = $(".editor-content, .collection-main-body, main, article").first();
  const fullText = contentArea.text();

  // Extract date
  let date: string | null = null;
  const dateMatch = fullText.match(/(\d{4}-\d{2}-\d{2})/);
  if (dateMatch?.[1]) {
    date = dateMatch[1];
  }

  // Determine action type
  const textLower = fullText.toLowerCase();
  let actionType: string | null = null;
  if (
    textLower.includes("sanktionsavgift") ||
    textLower.includes("administrative fine") ||
    textLower.includes("penalty fee")
  ) {
    actionType = "fine";
  } else if (
    textLower.includes("aterkallar") ||
    textLower.includes("withdraw") ||
    textLower.includes("revoke")
  ) {
    actionType = "ban";
  } else if (
    textLower.includes("forelagger") ||
    textLower.includes("restriction") ||
    textLower.includes("forbud")
  ) {
    actionType = "restriction";
  } else if (
    textLower.includes("varning") ||
    textLower.includes("warning") ||
    textLower.includes("anmarkning") ||
    textLower.includes("remark")
  ) {
    actionType = "warning";
  }

  // If both a fine and a warning/remark, classify as fine (the more severe action)
  if (actionType === "warning" && textLower.includes("sanktionsavgift")) {
    actionType = "fine";
  }

  // Extract amount in SEK
  let amount: number | null = null;
  const amountPatterns = [
    // "sanktionsavgift pa 500 miljoner kronor" / "50 miljoner kronor"
    /(\d[\d\s,.]*)\s*miljoner\s*kronor/i,
    // "sanktionsavgift pa 4 miljarder kronor"
    /(\d[\d\s,.]*)\s*miljarder?\s*kronor/i,
    // "fine of SEK 20 million" / "SEK 500 million"
    /SEK\s*(\d[\d\s,.]*)\s*million/i,
    // "SEK 4 billion"
    /SEK\s*(\d[\d\s,.]*)\s*billion/i,
    // "administrative fine of X kronor" (exact number)
    /(?:sanktionsavgift|fine|penalty)\s+(?:pa|of|om)\s+(\d[\d\s,.]+)\s*(?:kronor|SEK)/i,
  ];

  for (const pattern of amountPatterns) {
    const match = fullText.match(pattern);
    if (match?.[1]) {
      const numStr = match[1].replace(/\s/g, "").replace(",", ".");
      const num = parseFloat(numStr);
      if (!isNaN(num)) {
        if (pattern.source.includes("miljarder")) {
          amount = num * 1_000_000_000;
        } else if (pattern.source.includes("miljoner") || pattern.source.includes("million")) {
          amount = num * 1_000_000;
        } else if (pattern.source.includes("billion")) {
          amount = num * 1_000_000_000;
        } else {
          amount = num;
        }
        break;
      }
    }
  }

  // Build summary from paragraphs
  const paragraphs: string[] = [];
  contentArea.find("p").each((_i, el) => {
    const text = $(el).text().trim();
    if (text.length > 15) {
      paragraphs.push(text);
    }
  });
  const summary = paragraphs.slice(0, 5).join(" ").slice(0, 5000);

  // Extract FI reference number (FI Dnr or similar)
  let referenceNumber: string | null = null;
  const refPatterns = [
    /(?:FI\s*(?:Dnr|dnr|ref)\.?\s*)([\d-]+(?:\/\d+)?)/i,
    /(?:Diarienummer|Dnr|ref)[:\s]*([\d-]+(?:\/\d+)?)/i,
  ];
  for (const pattern of refPatterns) {
    const match = fullText.match(pattern);
    if (match?.[1]) {
      referenceNumber = `FI-${match[1]}`;
      break;
    }
  }

  // Extract FFFS references mentioned in the decision
  const fffsRefs: string[] = [];
  const fffsPattern = /FFFS\s+(\d{4}:\d+(?:\s+kap\.\d+(?:\s+par\.\d+)?)?)/g;
  let fffsMatch: RegExpExecArray | null;
  while ((fffsMatch = fffsPattern.exec(fullText)) !== null) {
    if (fffsMatch[1] && !fffsRefs.includes(`FFFS ${fffsMatch[1]}`)) {
      fffsRefs.push(`FFFS ${fffsMatch[1]}`);
    }
  }
  const sourcebookReferences = fffsRefs.length > 0 ? fffsRefs.join(", ") : null;

  // Try to extract a better firm name from the page itself
  const title = $("h1, h2").first().text().trim();
  const firmName = extractFirmNameFromHeading(title) || fallbackFirmName;

  return {
    firmName,
    date,
    actionType,
    amount,
    summary,
    referenceNumber,
    sourcebookReferences,
  };
}

// ── Publications page parsing (promemorior, tillsynsrapporter) ──────────────

interface PublicationEntry {
  title: string;
  date: string | null;
  description: string;
  url: string;
  type: "promemoria" | "tillsynsrapport" | "other";
}

function parsePublicationsPage(html: string): {
  entries: PublicationEntry[];
  nextPageUrl: string | null;
} {
  const $ = cheerio.load(html);
  const entries: PublicationEntry[] = [];

  // Publications follow a similar list pattern with headings and dates
  $("h3 a[href], h2 a[href]").each((_i, el) => {
    const $link = $(el);
    const href = $link.attr("href") ?? "";
    const title = $link.text().trim();

    if (!title || title.length < 5) return;

    // Skip navigation links
    if (href.startsWith("#") || href.includes("javascript:")) return;

    const $heading = $link.closest("h3, h2");
    let date: string | null = null;
    let description = "";

    // Look at following siblings for date and description
    let next = $heading.next();
    let sibCount = 0;
    while (next.length > 0 && !next.is("h3, h2") && sibCount < 5) {
      const sibText = next.text().trim();
      const dateMatch = sibText.match(/(\d{4}-\d{2}-\d{2})/);
      if (dateMatch?.[1] && !date) {
        date = dateMatch[1];
      } else if (sibText.length > 20 && !description) {
        description = sibText;
      }
      next = next.next();
      sibCount++;
    }

    // Classify the publication type
    const titleLower = title.toLowerCase();
    let type: PublicationEntry["type"] = "other";
    if (
      titleLower.includes("promemoria") ||
      titleLower.includes("pm") ||
      titleLower.includes("memorandum")
    ) {
      type = "promemoria";
    } else if (
      titleLower.includes("tillsynsrapport") ||
      titleLower.includes("supervisory report") ||
      titleLower.includes("supervision report")
    ) {
      type = "tillsynsrapport";
    }

    entries.push({ title, date, description, url: href, type });
  });

  // Check for pagination
  let nextPageUrl: string | null = null;
  $("a").each((_i, el) => {
    const href = $(el).attr("href") ?? "";
    const text = $(el).text().trim().toLowerCase();
    if (
      (text.includes("visa fler") || text.includes("show more") || text.includes("next")) &&
      href.includes("page=")
    ) {
      nextPageUrl = href.startsWith("http") ? href : href;
    }
  });

  return { entries, nextPageUrl };
}

// ── Counters ────────────────────────────────────────────────────────────────

interface IngestStats {
  fffsFound: number;
  fffsInserted: number;
  fffsSkipped: number;
  fffsErrors: number;
  sanctionsFound: number;
  sanctionsInserted: number;
  sanctionsSkipped: number;
  sanctionsErrors: number;
  publicationsFound: number;
  publicationsInserted: number;
}

function newStats(): IngestStats {
  return {
    fffsFound: 0,
    fffsInserted: 0,
    fffsSkipped: 0,
    fffsErrors: 0,
    sanctionsFound: 0,
    sanctionsInserted: 0,
    sanctionsSkipped: 0,
    sanctionsErrors: 0,
    publicationsFound: 0,
    publicationsInserted: 0,
  };
}

// ── Main ingestion: FFFS regulations ────────────────────────────────────────

async function ingestFffsRegulations(
  db: Database.Database | null,
  progress: Progress,
  cli: CliArgs,
  stats: IngestStats,
): Promise<void> {
  console.log("\n=== Phase 1: FFFS Regulations ===");
  console.log("Fetching FFFS list page...");

  const listUrl = `${BASE_URL}/en/our-registers/fffs/list-of-regulations/`;

  let html: string;
  try {
    html = await rateLimitedFetch(listUrl);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`Failed to fetch FFFS list: ${msg}`);
    return;
  }

  const entries = parseFffsListPage(html);
  console.log(`Found ${entries.length} FFFS entries on list page`);
  stats.fffsFound = entries.length;

  if (entries.length === 0) {
    console.warn("No FFFS entries parsed. The page structure may have changed.");
    return;
  }

  // Seed the FI_FFFS sourcebook
  if (db && !cli.dryRun) {
    upsertSourcebook(
      db,
      "FI_FFFS",
      "FI Foreskrifter och allmanna rad (FFFS)",
      "Finansinspektionens foreskrifter och allmanna rad — bindande regler och vagledning for finansiella foretag.",
    );
  }

  // Process each regulation's detail page
  for (const entry of entries) {
    const entryKey = `FFFS ${entry.number}`;

    if (cli.resume && progress.completed_fffs_detail.includes(entryKey)) {
      stats.fffsSkipped++;
      continue;
    }

    if (!entry.detailUrl) {
      console.warn(`  No detail URL for ${entryKey}, skipping`);
      stats.fffsErrors++;
      continue;
    }

    const detailUrl = entry.detailUrl.startsWith("http")
      ? entry.detailUrl
      : `${BASE_URL}${entry.detailUrl}`;

    console.log(`  Fetching ${entryKey}: ${entry.title.slice(0, 80)}...`);

    let detailHtml: string;
    try {
      detailHtml = await rateLimitedFetch(detailUrl);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`  Failed to fetch detail for ${entryKey}: ${msg}`);
      stats.fffsErrors++;
      continue;
    }

    const detail = parseFffsDetailPage(detailHtml);

    // Build the provision text from the detail page summary.
    // If the summary is too short, use the title as a fallback.
    const provisionText =
      detail.summary.length > 50
        ? detail.summary
        : `${detail.title}. ${detail.summary}`.trim();

    if (provisionText.length < 10) {
      console.warn(`  No content extracted for ${entryKey}, skipping`);
      stats.fffsErrors++;
      continue;
    }

    const reference = `FFFS ${entry.number}`;
    const provisionType = entry.type === "amendment" ? "andringsforeskrift" : "foreskrift";

    if (!cli.dryRun && db) {
      const inserted = insertProvision(db, {
        sourcebook_id: "FI_FFFS",
        reference,
        title: detail.title || entry.title,
        text: provisionText,
        type: provisionType,
        status: detail.status,
        effective_date: detail.effectiveDate,
        chapter: null,
        section: null,
      });

      if (inserted) {
        stats.fffsInserted++;
        console.log(`    Inserted: ${reference}`);
      } else {
        stats.fffsSkipped++;
        console.log(`    Already exists: ${reference}`);
      }

      // Also insert amendment entries as separate provisions if they have dates
      for (const amendment of detail.amendments) {
        if (amendment.number !== entry.number) {
          const amRef = `FFFS ${amendment.number}`;
          insertProvision(db, {
            sourcebook_id: "FI_FFFS",
            reference: amRef,
            title: `Amendment to ${reference}`,
            text: `Amendment FFFS ${amendment.number} to ${reference} (${detail.title || entry.title}).`,
            type: "andringsforeskrift",
            status: detail.status,
            effective_date: amendment.date,
            chapter: null,
            section: null,
          });
        }
      }
    } else if (cli.dryRun) {
      console.log(`    [dry-run] Would insert: ${reference} (${detail.status})`);
      if (detail.pdfUrls.length > 0) {
        console.log(`    [dry-run] PDFs: ${detail.pdfUrls.length}`);
      }
      stats.fffsInserted++;
    }

    // Save progress after each entry
    progress.completed_fffs_detail.push(entryKey);
    if (!cli.dryRun) {
      saveProgress(progress);
    }
  }

  console.log(
    `\nFFFS phase complete: ${stats.fffsInserted} inserted, ` +
      `${stats.fffsSkipped} skipped, ${stats.fffsErrors} errors`,
  );
}

// ── Main ingestion: Sanctions ───────────────────────────────────────────────

const SANCTION_CATEGORIES = [
  {
    id: "financial-firms-en",
    name: "Financial firms (EN)",
    url: "/en/published/sanctions/financial-firms/",
    years: [2026, 2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017],
  },
  {
    id: "financial-firms-sv",
    name: "Finansiella foretag (SV)",
    url: "/sv/publicerat/sanktioner/finansiella-foretag/",
    years: [2026, 2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017],
  },
  {
    id: "market-abuse-sv",
    name: "Marknadsmissbruk (SV)",
    url: "/sv/publicerat/sanktioner/marknadsmissbruk/",
    years: [2026, 2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017],
  },
  {
    id: "financial-reporting-en",
    name: "Financial reporting supervision (EN)",
    url: "/en/published/sanctions/financial-reporting-supervision/",
    years: [2026, 2025, 2024, 2023, 2022, 2021, 2020],
  },
];

async function ingestSanctions(
  db: Database.Database | null,
  progress: Progress,
  cli: CliArgs,
  stats: IngestStats,
): Promise<void> {
  console.log("\n=== Phase 2: Sanctions / Enforcement Actions ===");

  for (const category of SANCTION_CATEGORIES) {
    const categoryKey = category.id;

    if (cli.resume && progress.completed_sanction_categories.includes(categoryKey)) {
      console.log(`Skipping category: ${category.name} (already completed)`);
      continue;
    }

    console.log(`\nCategory: ${category.name}`);

    for (const year of category.years) {
      const yearPageUrl = `${BASE_URL}${category.url}?year=${year}`;
      const pageKey = `${categoryKey}:${year}`;

      if (cli.resume && progress.completed_sanction_pages.includes(pageKey)) {
        console.log(`  Skipping year ${year} (already completed)`);
        continue;
      }

      console.log(`  Year ${year}...`);

      // Paginate through all pages for this year
      let currentUrl: string | null = yearPageUrl;
      let pageNum = 1;

      while (currentUrl) {
        let html: string;
        try {
          const fetchUrl = currentUrl.startsWith("http")
            ? currentUrl
            : `${BASE_URL}${currentUrl}`;
          html = await rateLimitedFetch(fetchUrl);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.warn(`    Failed to fetch page ${pageNum}: ${msg}`);
          break;
        }

        const { entries, nextPageUrl } = parseSanctionsListPage(html);
        stats.sanctionsFound += entries.length;

        if (entries.length === 0 && pageNum === 1) {
          console.log(`    No entries found for ${year}`);
          break;
        }

        console.log(`    Page ${pageNum}: ${entries.length} entries`);

        // Fetch detail page for each sanction
        for (const entry of entries) {
          const detailUrl = entry.detailUrl.startsWith("http")
            ? entry.detailUrl
            : `${BASE_URL}${entry.detailUrl}`;

          let detail: SanctionDetail;

          try {
            const detailHtml = await rateLimitedFetch(detailUrl);
            detail = parseSanctionDetailPage(detailHtml, entry.firmName);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            console.warn(`      Failed to fetch detail for ${entry.firmName}: ${msg}`);

            // Use list-level data as fallback
            detail = {
              firmName: entry.firmName,
              date: entry.date,
              actionType: null,
              amount: null,
              summary: entry.description,
              referenceNumber: null,
              sourcebookReferences: null,
            };
            stats.sanctionsErrors++;
          }

          if (!cli.dryRun && db) {
            const inserted = insertEnforcement(db, {
              firm_name: detail.firmName,
              reference_number: detail.referenceNumber,
              action_type: detail.actionType,
              amount: detail.amount,
              date: detail.date ?? entry.date,
              summary: detail.summary || entry.description,
              sourcebook_references: detail.sourcebookReferences,
            });

            if (inserted) {
              stats.sanctionsInserted++;
              const amountStr = detail.amount
                ? ` (SEK ${detail.amount.toLocaleString("sv-SE")})`
                : "";
              console.log(
                `      Inserted: ${detail.firmName} [${detail.actionType ?? "unknown"}]${amountStr}`,
              );
            } else {
              stats.sanctionsSkipped++;
            }
          } else if (cli.dryRun) {
            console.log(
              `      [dry-run] Would insert: ${detail.firmName} (${detail.date ?? "no date"})`,
            );
            stats.sanctionsInserted++;
          }
        }

        // Move to next page if available
        currentUrl = nextPageUrl;
        pageNum++;
      }

      progress.completed_sanction_pages.push(pageKey);
      if (!cli.dryRun) {
        saveProgress(progress);
      }
    }

    progress.completed_sanction_categories.push(categoryKey);
    if (!cli.dryRun) {
      saveProgress(progress);
    }
  }

  console.log(
    `\nSanctions phase complete: ${stats.sanctionsInserted} inserted, ` +
      `${stats.sanctionsSkipped} skipped, ${stats.sanctionsErrors} errors`,
  );
}

// ── Main ingestion: Publications (promemorior, tillsynsrapporter) ───────────

async function ingestPublications(
  db: Database.Database | null,
  progress: Progress,
  cli: CliArgs,
  stats: IngestStats,
): Promise<void> {
  console.log("\n=== Phase 3: Publications (Promemorior & Tillsynsrapporter) ===");

  // Seed the additional sourcebooks
  if (db && !cli.dryRun) {
    upsertSourcebook(
      db,
      "FI_PROMEMORIOR",
      "FI Promemorior",
      "Finansinspektionens promemorior om operativ resiliens, IT-risk och tillsynsprinciper.",
    );
    upsertSourcebook(
      db,
      "FI_TILLSYNSRAPPORTER",
      "FI Tillsynsrapporter",
      "Finansinspektionens tillsynsrapporter om iakttagelser och prioriterade omraden for tillsyn.",
    );
  }

  // FI publishes these under /en/published/ and /sv/publicerat/
  const publicationUrls = [
    `${BASE_URL}/en/published/reports/`,
    `${BASE_URL}/sv/publicerat/rapporter/`,
  ];

  for (const pubUrl of publicationUrls) {
    console.log(`\nFetching publications from: ${pubUrl}`);

    let currentUrl: string | null = pubUrl;
    let pageNum = 1;
    const maxPages = 10; // Safety limit

    while (currentUrl && pageNum <= maxPages) {
      let html: string;
      try {
        const fetchUrl = currentUrl.startsWith("http")
          ? currentUrl
          : `${BASE_URL}${currentUrl}`;
        html = await rateLimitedFetch(fetchUrl);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.warn(`  Failed to fetch publications page ${pageNum}: ${msg}`);
        break;
      }

      const { entries, nextPageUrl } = parsePublicationsPage(html);
      stats.publicationsFound += entries.length;

      if (entries.length === 0) {
        console.log(`  No entries found on page ${pageNum}`);
        break;
      }

      console.log(`  Page ${pageNum}: ${entries.length} publications`);

      for (const entry of entries) {
        if (entry.type === "other") continue; // Only interested in promemorior and tillsynsrapporter

        const sourcebookId = entry.type === "promemoria" ? "FI_PROMEMORIOR" : "FI_TILLSYNSRAPPORTER";
        const reference = `FI_${entry.type === "promemoria" ? "PM" : "TR"} ${entry.date?.slice(0, 4) ?? "XXXX"}`;

        if (!cli.dryRun && db) {
          const inserted = insertProvision(db, {
            sourcebook_id: sourcebookId,
            reference: `${reference} — ${entry.title.slice(0, 100)}`,
            title: entry.title,
            text: entry.description || entry.title,
            type: entry.type,
            status: "in_force",
            effective_date: entry.date,
            chapter: null,
            section: null,
          });

          if (inserted) {
            stats.publicationsInserted++;
            console.log(`    Inserted: ${entry.title.slice(0, 70)}...`);
          }
        } else if (cli.dryRun) {
          console.log(`    [dry-run] Would insert: ${entry.title.slice(0, 70)}...`);
          stats.publicationsInserted++;
        }
      }

      currentUrl = nextPageUrl;
      pageNum++;
    }
  }

  console.log(`\nPublications phase complete: ${stats.publicationsInserted} inserted`);
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const cli = parseArgs();
  const stats = newStats();

  console.log("Finansinspektionen (FI) Ingestion Crawler");
  console.log("=========================================");
  console.log(`Database: ${DB_PATH}`);
  console.log(`Progress: ${PROGRESS_PATH}`);
  console.log(
    `Mode: ${cli.dryRun ? "DRY RUN" : "LIVE"} | ` +
      `Resume: ${cli.resume ? "yes" : "no"} | ` +
      `Force: ${cli.force ? "yes" : "no"}`,
  );
  console.log(`Rate limit: ${RATE_LIMIT_MS}ms between requests`);
  console.log();

  // Ensure data directory exists
  const dataDir = dirname(DB_PATH);
  if (!existsSync(dataDir)) {
    mkdirSync(dataDir, { recursive: true });
    console.log(`Created data directory: ${dataDir}`);
  }

  // Initialize database (skip in dry-run mode)
  let db: Database.Database | null = null;
  if (!cli.dryRun) {
    db = initDb(cli.force);
    console.log(`Database initialised at ${DB_PATH}`);
  } else {
    console.log("[dry-run] Skipping database initialisation");
  }

  // Load or reset progress
  const progress = cli.force
    ? {
        completed_fffs_pages: [] as string[],
        completed_fffs_detail: [] as string[],
        completed_sanction_categories: [] as string[],
        completed_sanction_pages: [] as string[],
        last_updated: new Date().toISOString(),
      }
    : loadProgress();

  if (cli.resume) {
    console.log(
      `Resuming from checkpoint: ` +
        `${progress.completed_fffs_detail.length} FFFS detail pages, ` +
        `${progress.completed_sanction_pages.length} sanction pages already done`,
    );
  }

  const startTime = Date.now();

  try {
    // Phase 1: FFFS Regulations
    if (!cli.sanctionsOnly) {
      await ingestFffsRegulations(db, progress, cli, stats);
    }

    // Phase 2: Sanctions / Enforcement Actions
    if (!cli.fffsOnly) {
      await ingestSanctions(db, progress, cli, stats);
    }

    // Phase 3: Publications (promemorior, tillsynsrapporter)
    if (!cli.fffsOnly && !cli.sanctionsOnly) {
      await ingestPublications(db, progress, cli, stats);
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`\nFatal error during ingestion: ${msg}`);
    if (err instanceof Error && err.stack) {
      console.error(err.stack);
    }
  }

  // Print summary
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log("\n" + "=".repeat(60));
  console.log("Ingestion Summary");
  console.log("=".repeat(60));
  console.log(`Duration: ${elapsed}s`);
  console.log();
  console.log("FFFS Regulations:");
  console.log(`  Found:    ${stats.fffsFound}`);
  console.log(`  Inserted: ${stats.fffsInserted}`);
  console.log(`  Skipped:  ${stats.fffsSkipped}`);
  console.log(`  Errors:   ${stats.fffsErrors}`);
  console.log();
  console.log("Enforcement Actions:");
  console.log(`  Found:    ${stats.sanctionsFound}`);
  console.log(`  Inserted: ${stats.sanctionsInserted}`);
  console.log(`  Skipped:  ${stats.sanctionsSkipped}`);
  console.log(`  Errors:   ${stats.sanctionsErrors}`);
  console.log();
  console.log("Publications:");
  console.log(`  Found:    ${stats.publicationsFound}`);
  console.log(`  Inserted: ${stats.publicationsInserted}`);

  if (db) {
    const provisionCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions").get() as { cnt: number }
    ).cnt;
    const sourcebookCount = (
      db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as { cnt: number }
    ).cnt;
    const enforcementCount = (
      db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as {
        cnt: number;
      }
    ).cnt;

    console.log();
    console.log("Database totals:");
    console.log(`  Sourcebooks:          ${sourcebookCount}`);
    console.log(`  Provisions:           ${provisionCount}`);
    console.log(`  Enforcement actions:  ${enforcementCount}`);

    db.close();
  }

  console.log();
  console.log(`Done. Progress saved at ${PROGRESS_PATH}`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
