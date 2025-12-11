// sync_last6h_append.js (REPLACE ENTIRE FILE WITH THIS)
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
  GSA_KEY_FILE,
  SHEET_NAME = "Sheet1"
} = process.env;

if (!MONGO_URI || !SPREADSHEET_ID || !GSA_KEY_FILE) {
  console.error("Missing required env vars. Check MONGO_URI, SPREADSHEET_ID, GSA_KEY_FILE in .env");
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
  // Prefer JSON from env (safer for cloud). Fallback to file path if provided.
  let keyJson;
  if (process.env.GSA_KEY_JSON) {
    try {
      keyJson = typeof process.env.GSA_KEY_JSON === "string"
        ? JSON.parse(process.env.GSA_KEY_JSON)
        : process.env.GSA_KEY_JSON;
    } catch (e) {
      throw new Error("GSA_KEY_JSON is not valid JSON: " + e.message);
    }
  } else if (process.env.GSA_KEY_FILE) {
    const keyPath = path.resolve(process.env.GSA_KEY_FILE);
    if (!fs.existsSync(keyPath)) throw new Error("Service account key file not found: " + keyPath);
    keyJson = JSON.parse(fs.readFileSync(keyPath, "utf8"));
  } else {
    throw new Error("No service account credentials found. Set GSA_KEY_JSON or GSA_KEY_FILE.");
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
    const sheets = await getSheetsClient();
    const client = new MongoClient(MONGO_URI);
    await client.connect();

    const db = client.db(MONGO_DB);
    const coll = db.collection(MONGO_COLLECTION);

    const sixHoursAgo = new Date(Date.now() - 6 * 60 * 60 * 1000);
    const oidCutoff = ObjectId.createFromTime(Math.floor(sixHoursAgo.getTime() / 1000));

    // Query docs from last 6 hours using ObjectId timestamp
    const cursor = coll.find({ _id: { $gte: oidCutoff } }).sort({ _id: 1 });
    const docs = await cursor.toArray();

    if (!docs || docs.length === 0) {
      console.log("No new records in last 6 hours. Exiting.");
      await client.close();
      return;
    }

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
      // If 400/404 or empty, we'll write header later
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
  } catch (err) {
    console.error("Fatal error:", err.message || err);
    console.error(err);
    process.exit(1);
  }
}

main();
