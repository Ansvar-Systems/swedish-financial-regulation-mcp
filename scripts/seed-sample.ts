/**
 * Seed the Swedish Financial Regulation database with sample provisions for testing.
 *
 * Inserts provisions from FI_FFFS (FFFS regulations), FI_Promemorior,
 * and FI_Tillsynsrapporter sourcebooks.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["FI_SE_DB_PATH"] ?? "data/fi-se.db";
const force = process.argv.includes("--force");

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

console.log(`Database initialised at ${DB_PATH}`);

interface SourcebookRow {
  id: string;
  name: string;
  description: string;
}

const sourcebooks: SourcebookRow[] = [
  {
    id: "FI_FFFS",
    name: "FI Foreskrifter och allmanna rad (FFFS)",
    description: "Finansinspektionens foreskrifter och allmanna rad — bindande regler och vagledning for finansiella foretag.",
  },
  {
    id: "FI_PROMEMORIOR",
    name: "FI Promemorior",
    description: "Finansinspektionens promemorior om operativ resiliens, IT-risk och tillsynsprinciper.",
  },
  {
    id: "FI_TILLSYNSRAPPORTER",
    name: "FI Tillsynsrapporter",
    description: "Finansinspektionens tillsynsrapporter om iakttagelser och prioriterade omraden for tillsyn.",
  },
];

const insertSourcebook = db.prepare(
  "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
);

for (const sb of sourcebooks) {
  insertSourcebook.run(sb.id, sb.name, sb.description);
}

console.log(`Inserted ${sourcebooks.length} sourcebooks`);

interface ProvisionRow {
  sourcebook_id: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effective_date: string;
  chapter: string;
  section: string;
}

const provisions: ProvisionRow[] = [
  {
    sourcebook_id: "FI_FFFS",
    reference: "FFFS 2014:1 kap.2 par.1",
    title: "Krav pa styrdokument for riskhantering",
    text: "Ett foretag ska ha skriftliga riktlinjer och instruktioner for riskhantering. Riktlinjerna ska godkanna av styrelsen och beskriva foretagets risktolerans, riskaptit och principer for identifiering, matning, styrning och kontroll av de risker som foretaget ar exponerat mot.",
    type: "foreskrift",
    status: "in_force",
    effective_date: "2014-04-01",
    chapter: "2",
    section: "1",
  },
  {
    sourcebook_id: "FI_FFFS",
    reference: "FFFS 2014:1 kap.3 par.2",
    title: "Styrelsens ansvar for intern kontroll",
    text: "Styrelsen ansvarar for att det finns en andamalsenlig intern kontroll. Styrelsen ska minst arligen utvardera effektiviteten i den interna kontrollen och vid behov vidta atgarder for att avhjapa identifierade brister.",
    type: "foreskrift",
    status: "in_force",
    effective_date: "2014-04-01",
    chapter: "3",
    section: "2",
  },
  {
    sourcebook_id: "FI_FFFS",
    reference: "FFFS 2014:1 kap.4 par.1",
    title: "Compliance-funktion",
    text: "Ett foretag ska ha en funktion for regelefterlevnad (compliance) som ar oberoende fran den operativa verksamheten. Compliance-funktionen ska identifiera och bedomma risken for att foretaget inte foljer tillamplika regler och ge rad till styrelse och anstallda om regelefterlevnad.",
    type: "foreskrift",
    status: "in_force",
    effective_date: "2014-04-01",
    chapter: "4",
    section: "1",
  },
  {
    sourcebook_id: "FI_FFFS",
    reference: "FFFS 2014:5 kap.2 par.3",
    title: "Krav pa IT-sakerhets- och kontinuitetsplanering",
    text: "Foretaget ska ha ett adekvat skydd mot informationssakerhetsrisker och ha planer for att hantera avbrott i IT-system. Kontinuitetsplanerna ska testas regelbudet och hallas aktuella. Allvarliga IT-incidenter och driftsavbrott ska rapporteras till Finansinspektionen.",
    type: "foreskrift",
    status: "in_force",
    effective_date: "2014-09-01",
    chapter: "2",
    section: "3",
  },
  {
    sourcebook_id: "FI_FFFS",
    reference: "FFFS 2014:5 kap.3 par.1",
    title: "Utlagd verksamhet och tredjepartsrisker",
    text: "Nar ett foretag lagger ut en viktig funktion eller verksamhet till en tredjepart, ska foretaget skertstalla att outsourcingavtalet ger foretaget mojlighet att overvaka och styra verksamheten. Foretaget kvarstar ansvarigt for efterlevnad av tillamplika regler.",
    type: "foreskrift",
    status: "in_force",
    effective_date: "2014-09-01",
    chapter: "3",
    section: "1",
  },
  {
    sourcebook_id: "FI_PROMEMORIOR",
    reference: "FI_PM 2021:12 avsnitt.3",
    title: "FI:s syn pa operativ resiliens for finansiella foretag",
    text: "Finansinspektionen forvanter sig att finansiella foretag identifierar sina viktiga affarsverksamheter och de IT-system och processer som dessa beroende av. Foretagen ska satta toleransgranser for hur lange de kan tala avbrott i dessa verksamheter.",
    type: "promemoria",
    status: "in_force",
    effective_date: "2021-06-01",
    chapter: "3",
    section: "3",
  },
  {
    sourcebook_id: "FI_PROMEMORIOR",
    reference: "FI_PM 2020:8 avsnitt.2",
    title: "Hantering av cyberrisker i finanssektorn",
    text: "FI:s tillsyn visar att foretag behover forbattra sin formaga att hantera cyberrisker. Detta inkluderar att kartlagga och skydda kritiska tillgangar, ha beredskap for att detektera cyberincidenter och genomfora regelbudna ovningar for incidenthantering.",
    type: "promemoria",
    status: "in_force",
    effective_date: "2020-10-01",
    chapter: "2",
    section: "2",
  },
  {
    sourcebook_id: "FI_TILLSYNSRAPPORTER",
    reference: "FI_TR 2023:3 avsnitt.4",
    title: "Tillsynsiakttagelser om kapitaltackning",
    text: "FI:s tillsynsarbete under 2022-2023 visar att flera banker haft brister i sina interna processer for bedommning av kapitalbehovet (ICAAP). Vanliga brister inkluderar otillracklig stresstestning av kapitalbehovet och bristfalling dokumentation av antaganden.",
    type: "tillsynsrapport",
    status: "in_force",
    effective_date: "2023-04-01",
    chapter: "4",
    section: "4",
  },
];

const insertProvision = db.prepare(`
  INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertAll = db.transaction(() => {
  for (const p of provisions) {
    insertProvision.run(
      p.sourcebook_id, p.reference, p.title, p.text,
      p.type, p.status, p.effective_date, p.chapter, p.section,
    );
  }
});

insertAll();

console.log(`Inserted ${provisions.length} sample provisions`);

interface EnforcementRow {
  firm_name: string;
  reference_number: string;
  action_type: string;
  amount: number;
  date: string;
  summary: string;
  sourcebook_references: string;
}

const enforcements: EnforcementRow[] = [
  {
    firm_name: "Nordea Bank AB",
    reference_number: "FI-2022-01234",
    action_type: "fine",
    amount: 50000000,
    date: "2022-06-15",
    summary: "Finansinspektionen beslutade om en sanktionsavgift pa 50 miljoner kronor mot Nordea Bank AB for brister i bankens atgarder mot penningtvatt och finansiering av terrorism.",
    sourcebook_references: "FFFS 2014:1 kap.3 par.2",
  },
  {
    firm_name: "Swedbank AB",
    reference_number: "FI-2020-05678",
    action_type: "fine",
    amount: 4000000000,
    date: "2020-03-19",
    summary: "Finansinspektionen alade Swedbank en sanktionsavgift pa 4 miljarder kronor for allvarliga brister i bankens atgarder mot penningtvatt med systematiska brister i kundkannedom och transaktionsovervakning for kunder i Baltikum.",
    sourcebook_references: "FFFS 2014:1 kap.4 par.1, FFFS 2014:5 kap.3 par.1",
  },
];

const insertEnforcement = db.prepare(`
  INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const insertEnforcementsAll = db.transaction(() => {
  for (const e of enforcements) {
    insertEnforcement.run(
      e.firm_name, e.reference_number, e.action_type, e.amount,
      e.date, e.summary, e.sourcebook_references,
    );
  }
});

insertEnforcementsAll();

console.log(`Inserted ${enforcements.length} sample enforcement actions`);

const provisionCount = (db.prepare("SELECT count(*) as cnt FROM provisions").get() as { cnt: number }).cnt;
const sourcebookCount = (db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as { cnt: number }).cnt;
const enforcementCount = (db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as { cnt: number }).cnt;
const ftsCount = (db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as { cnt: number }).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Sourcebooks:          ${sourcebookCount}`);
console.log(`  Provisions:           ${provisionCount}`);
console.log(`  Enforcement actions:  ${enforcementCount}`);
console.log(`  FTS entries:          ${ftsCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
