// debug_key.js
import fs from "fs";
import path from "path";
import dotenv from "dotenv";
dotenv.config();

console.log("---- ENV VARS ----");
console.log("GSA_KEY_FILE =", process.env.GSA_KEY_FILE);
console.log("SPREADSHEET_ID =", process.env.SPREADSHEET_ID ? "<present>" : "<missing>");
console.log("MONGO_URI present? ", !!process.env.MONGO_URI);
console.log();

if (!process.env.GSA_KEY_FILE) {
  console.error("ERROR: GSA_KEY_FILE is undefined. Open .env and set GSA_KEY_FILE to the full path.");
  process.exit(1);
}

const keyPath = path.resolve(process.env.GSA_KEY_FILE);
console.log("Resolved keyPath =", keyPath);

if (!fs.existsSync(keyPath)) {
  console.error("ERROR: key file NOT found at that path.");
  process.exit(2);
}

let raw;
try {
  raw = fs.readFileSync(keyPath, "utf8");
} catch (e) {
  console.error("ERROR: cannot read key file:", e.message);
  process.exit(3);
}

let json;
try {
  json = JSON.parse(raw);
} catch (e) {
  console.error("ERROR: key file is not valid JSON:", e.message);
  process.exit(4);
}

console.log("Key JSON loaded. Checking required fields...");
console.log("client_email:", !!json.client_email ? json.client_email : "<missing>");
console.log("private_key length:", json.private_key ? json.private_key.length : "<missing>");
console.log("\nIf client_email and private_key are present, the key file is valid.");
