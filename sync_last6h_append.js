// sync_last6h_append.js
import { MongoClient, ObjectId } from "mongodb";
import fs from "fs";
import path from "path";
import dotenv from "dotenv";
import { google } from "googleapis";

dotenv.config();

// --- Environment ---
const {
  MONGO_URI,
  MONGO_DB = "prod",
  MONGO_COLLECTION = "ipcregistrations",
  SPREADSHEET_ID,
  SHEET_NAME = "Sheet1",
  GSA_KEY_JSON,
  GSA_KEY_FILE
} = process.env;

// Validate required envs (accept GSA_KEY_JSON OR GSA_KEY_FILE)
if (!MONGO_URI || !SPREADSHEET_ID || (!GSA_KEY_JSON && !GSA_KEY_FILE)) {
  console.error("Missing required env vars.");
  console.error("You must set:");
  console.error("  - MONGO_URI");
  console.error("  - SPREADSHEET_ID");
  console.error("  - and either GSA_KEY_JSON (paste full JSON) OR GSA_KEY_FILE (path to json file)");
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
    // Could be raw JSON string or already parsed object
    try {
      keyJson = typeof GSA_KEY_JSON === "string" ? JSON.parse(GSA_KEY_JSON) : GSA_KEY_JSON;
    } catch (err) {
      throw new Error("Failed to parse GSA_KEY_JSON: " + err.message);
    }
  } else if (GSA_KEY_FILE) {
    const keyPath = path.resolve(GSA_KEY_FILE);
    if (!fs.existsSync(keyPath)) throw new Error("Service account key file not found: " + keyPath);
    try {
      keyJson = JSON.parse(fs.readFileSync(keyPath, "utf8"));
    } catch (err) {
      throw new Error("Failed to read/parse key file: " + err.message);
    }
  } else {
    throw new Error("No Google service account credentials found (GSA_KEY_JSON or GSA_KEY_FILE).");
  }

  // Minimal sanity check
  if (!keyJson.client_email || !keyJson.private_key) {
    throw new Error("Service account JSON missing client_email or private_key.");
  }

  const auth = new google.auth.GoogleAuth({
    credentials: keyJson,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  const authClient = await auth.getClient();
  return google.sheets({ version: "v4", auth: authClient });
}

async function main() {
  console.log("Starting sync job");
  console.log("MONGO_DB:", MONGO_DB);
  console.log("MONGO_COLLECTION:", MONGO_COLLECTION);
  console.log("SHEET_NAME:", SHEET_NAME);
  console.log("SPREADSHEET_ID present:", !!SPREADSHEET_ID);
  console.log("Using GSA_KEY_JSON:", !!GSA_KEY_JSON, "GSA_KEY_FILE:", !!GSA_KEY_FILE);

  try {
    const sheets = await getSheetsClient();
    console.log("Authenticated with Google Sheets.");

    const client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 10000 });
    await client.connect();
    console.log("Connected to MongoDB.");

    const db = client.db(MONGO_DB);
    const coll = db.collection(MONGO_COLLECTION);

    const sixHoursAgo = new Date(Date.now() - 6 * 60 * 60 * 1000);
    const oidCutoff = ObjectId.createFromTime(Math.floor(sixHoursAgo.getTime() / 1000));

    console.log("Querying documents with _id >=", oidCutoff.toString());
    const cursor = coll.find({ _id: { $gte: oidCutoff } }).sort({ _id: 1 });
    const docs = await cursor.toArray();

    if (!docs || docs.length === 0) {
      console.log("No new records in last 6 hours. Exiting.");
      await client.close();
      return;
    }

    console.log(`Found ${docs.length} document(s) to append.`);

    // Flatten docs and collect headers
    const rowsFlat = [];
    const headerSet = new Set();
    for (const d of docs) {
      const copy = { ...d };
      if (copy._id) copy._id = copy._id.toString();
      const flat = flatten(copy);
      rowsFlat.push(flat);
      Object.keys(flat).forEach(k => headerSet.add(k));
    }

    // Get existing header row from sheet (if any)
    const headerRange = `${SHEET_NAME}!A1:1`;
    let existingHeader = [];
    try {
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: headerRange
      });
      existingHeader = (res.data.values && res.data.values[0]) || [];
    } catch (e) {
      // ignore errors (sheet empty or not present)
      existingHeader = [];
    }

    // Build headers: keep existing header order and append any new columns
    let headers = existingHeader.length > 0 ? existingHeader.slice() : Array.from(headerSet);
    if (existingHeader.length > 0) {
      const missing = Array.from(headerSet).filter(h => !headers.includes(h));
      headers = headers.concat(missing);
    }

    // If there was no header, write header first
    if (existingHeader.length === 0) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: headerRange,
        valueInputOption: "RAW",
        requestBody: { values: [headers] }
      });
      console.log("Header row written.");
    } else if (existingHeader.length < headers.length) {
      // update header if we added new columns
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: headerRange,
        valueInputOption: "RAW",
        requestBody: { values: [headers] }
      });
      console.log("Header row updated with new columns.");
    } else {
      console.log("Existing header found; appending data rows only.");
    }

    // Build rows aligned to headers
    const values = rowsFlat.map(obj => headers.map(h => {
      const v = Object.prototype.hasOwnProperty.call(obj, h) ? obj[h] : "";
      if (Array.isArray(v) || (typeof v === "object" && v !== null)) {
        try { return JSON.stringify(v); } catch { return String(v); }
      }
      return v === null || v === undefined ? "" : String(v);
    }));

    // Append rows after header
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!A2`,
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      requestBody: { values }
    });

    console.log(`Appended ${values.length} rows to sheet.`);
    await client.close();
    console.log("Done.");
  } catch (err) {
    console.error("Fatal error:", err && (err.message || String(err)));
    if (err && err.response && err.response.data) {
      console.error("API response:", JSON.stringify(err.response.data));
    } else {
      console.error(err);
    }
    process.exit(1);
  }
}

main();
