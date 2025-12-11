// sync_last6h_append.js (NO DUPLICATES VERSION)
import { MongoClient, ObjectId } from "mongodb";
import fs from "fs";
import path from "path";
import dotenv from "dotenv";
import { google } from "googleapis";

dotenv.config();

const {
  MONGO_URI,
  MONGO_DB = "prod",
  MONGO_COLLECTION = "ipcregistrations",
  SPREADSHEET_ID,
  GSA_KEY_JSON,
  GSA_KEY_FILE,
  SHEET_NAME = "Sheet1"
} = process.env;

if (!MONGO_URI || !SPREADSHEET_ID || (!GSA_KEY_JSON && !GSA_KEY_FILE)) {
  console.error("Missing required env vars. Check MONGO_URI, SPREADSHEET_ID, GSA_KEY_JSON/GSA_KEY_FILE");
  process.exit(1);
}

function flatten(obj, prefix = "", out = {}) {
  if (obj === null || obj === undefined) {
    out[prefix.replace(/\.$/, "")] = obj;
    return out;
  }
  if (typeof obj !== "object" || obj instanceof Date || Array.isArray(obj)) {
    out[prefix.replace(/\.$/, "")] = obj;
    return out;
  }
  for (const key of Object.keys(obj)) {
    flatten(obj[key], prefix ? `${prefix}${key}.` : `${key}.`, out);
  }
  return out;
}

async function getSheetsClient() {
  let keyJson;

  if (GSA_KEY_JSON) {
    keyJson = JSON.parse(GSA_KEY_JSON);
  } else {
    const keyPath = path.resolve(GSA_KEY_FILE);
    keyJson = JSON.parse(fs.readFileSync(keyPath, "utf8"));
  }

  const auth = new google.auth.GoogleAuth({
    credentials: keyJson,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  const authClient = await auth.getClient();
  return google.sheets({ version: "v4", auth: authClient });
}

async function main() {
  try {
    console.log("Starting sync...");
    const sheets = await getSheetsClient();
    const client = new MongoClient(MONGO_URI);
    await client.connect();

    const db = client.db(MONGO_DB);
    const coll = db.collection(MONGO_COLLECTION);

    // STEP 1: Get all existing IDs from sheet
    console.log("Fetching existing _id list from sheet...");
    let existingIds = new Set();
    try {
      const read = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A2:A`
      });

      if (read.data.values) {
        read.data.values.forEach(row => {
          if (row[0]) existingIds.add(row[0]);
        });
      }
    } catch (err) {
      console.log("Sheet empty or header only. No existing IDs.");
    }

    console.log(`Found ${existingIds.size} IDs already in sheet.`);

    // STEP 2: Fetch last 6 hours MongoDB documents
    const sixHoursAgo = new Date(Date.now() - 6 * 60 * 60 * 1000);
    const oidCutoff = ObjectId.createFromTime(Math.floor(sixHoursAgo.getTime() / 1000));

    const docs = await coll.find({ _id: { $gte: oidCutoff } }).sort({ _id: 1 }).toArray();

    console.log(`Mongo returned ${docs.length} docs.`);

    // STEP 3: Remove docs whose _id already exists
    const newDocs = docs.filter(d => !existingIds.has(d._id.toString()));

    console.log(`New docs to append: ${newDocs.length}`);

    if (newDocs.length === 0) {
      console.log("No new records. Exiting.");
      await client.close();
      return;
    }

    // STEP 4: Flatten & build header list
    const rowsFlat = [];
    const headerSet = new Set();

    for (const d of newDocs) {
      const copy = { ...d };
      copy._id = copy._id.toString();
      const flat = flatten(copy);
      rowsFlat.push(flat);
      Object.keys(flat).forEach(k => headerSet.add(k));
    }

    // STEP 5: Read existing header or create one
    let existingHeader = [];
    try {
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A1:1`
      });
      existingHeader = (res.data.values && res.data.values[0]) || [];
    } catch {}

    let headers = existingHeader.length ? [...existingHeader] : [...headerSet];
    if (existingHeader.length) {
      const missing = [...headerSet].filter(h => !headers.includes(h));
      headers = headers.concat(missing);
    }

    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: { values: [headers] }
    });

    // STEP 6: Convert new rows into ordered format
    const values = rowsFlat.map(obj => headers.map(h => {
      const v = obj[h];
      if (Array.isArray(v) || (typeof v === "object" && v !== null)) return JSON.stringify(v);
      return v === undefined ? "" : v;
    }));

    // STEP 7: Append to sheet
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!A2`,
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      requestBody: { values }
    });

    console.log(`Successfully appended ${values.length} new rows.`);
    await client.close();

  } catch (err) {
    console.error("Fatal error:", err);
    process.exit(1);
  }
}

main();
