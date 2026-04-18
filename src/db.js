const fs = require("fs/promises");
const path = require("path");
const initSqlJs = require("sql.js");

const DB_DIR = path.join(__dirname, "..", "data");
const DB_PATH = path.join(DB_DIR, "postventa.sqlite");

let SQL;
let db;

async function ensureReady() {
  if (!SQL) {
    SQL = await initSqlJs();
  }

  if (!db) {
    await fs.mkdir(DB_DIR, { recursive: true });
    try {
      const fileBuffer = await fs.readFile(DB_PATH);
      db = new SQL.Database(fileBuffer);
    } catch {
      db = new SQL.Database();
    }
  }
}

/** Cierra la instancia en memoria y vuelve a leer el archivo (p. ej. tras ejecutar init-db en otro proceso). */
async function reloadFromDisk() {
  if (!SQL) {
    SQL = await initSqlJs();
  }
  await fs.mkdir(DB_DIR, { recursive: true });
  try {
    const fileBuffer = await fs.readFile(DB_PATH);
    if (db) {
      db.close();
    }
    db = new SQL.Database(fileBuffer);
  } catch {
    if (db) {
      db.close();
    }
    db = new SQL.Database();
    await persist();
  }
}

async function persist() {
  const data = db.export();
  await fs.writeFile(DB_PATH, Buffer.from(data));
}

function mapResult(statement) {
  const rows = [];
  while (statement.step()) {
    rows.push(statement.getAsObject());
  }
  return rows;
}

async function run(sql, params = []) {
  await ensureReady();
  db.run(sql, params);
  await persist();
}

async function all(sql, params = []) {
  await ensureReady();
  const statement = db.prepare(sql, params);
  const rows = mapResult(statement);
  statement.free();
  return rows;
}

async function get(sql, params = []) {
  const rows = await all(sql, params);
  return rows[0] || null;
}

module.exports = {
  run,
  all,
  get,
  ensureReady,
  reloadFromDisk,
  DB_PATH,
};
