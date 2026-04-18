const express = require("express");
const session = require("express-session");
const PDFDocument = require("pdfkit");
const multer = require("multer");
const fs = require("fs");
const path = require("path");
const QRCode = require("qrcode");
const { all, get, run, reloadFromDisk } = require("./db");

const app = express();
const PORT = process.env.PORT || 3000;
const uploadsRoot = path.join(__dirname, "..", "uploads", "products");
const ingressDocsRoot = path.join(__dirname, "..", "uploads", "ingress-docs");

if (!fs.existsSync(uploadsRoot)) {
  fs.mkdirSync(uploadsRoot, { recursive: true });
}
if (!fs.existsSync(ingressDocsRoot)) {
  fs.mkdirSync(ingressDocsRoot, { recursive: true });
}

const upload = multer({
  storage: multer.diskStorage({
    destination: uploadsRoot,
    filename: (_, file, cb) => {
      const safe = file.originalname.replace(/[^a-zA-Z0-9._-]/g, "_");
      cb(null, `${Date.now()}_${safe}`);
    },
  }),
});

const uploadIngressDoc = multer({
  storage: multer.diskStorage({
    destination: ingressDocsRoot,
    filename: (_, file, cb) => {
      const safe = file.originalname.replace(/[^a-zA-Z0-9._-]/g, "_");
      cb(null, `${Date.now()}_${safe}`);
    },
  }),
  limits: { fileSize: 12 * 1024 * 1024 },
  fileFilter: (_, file, cb) => {
    const ok =
      file.mimetype === "application/pdf" ||
      file.mimetype === "application/octet-stream" ||
      (file.originalname && String(file.originalname).toLowerCase().endsWith(".pdf"));
    cb(null, Boolean(ok));
  },
});

app.use(express.urlencoded({ extended: true }));
app.use("/uploads", express.static(path.join(__dirname, "..", "uploads")));
app.use(
  session({
    secret: process.env.SESSION_SECRET || "postventa_secret_demo",
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 8 * 60 * 60 * 1000, sameSite: "lax" },
  })
);

function escapeHtml(value = "") {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

/** Etiqueta legible para PDF / UI */
function formatDocStatus(status) {
  const s = String(status || "-");
  const map = {
    PENDIENTE_APROBACION: "Pendiente aprobacion",
    PENDIENTE_APROB_INGRESO: "Pendiente aprobacion de ingreso",
    INGRESO_RECHAZADO: "Ingreso rechazado",
    INGRESADA: "Ingresada",
    RECHAZADA: "Rechazada",
    PENDIENTE: "Pendiente",
    APROBADA: "Aprobada",
    ATENDIDA: "Atendida",
  };
  return map[s] || s.replace(/_/g, " ");
}

/** Serie repetida dentro del mismo grupo marca+categoria+modelo (empresa). */
async function findSerialConflictSameProductGroup(companyId, targetProductId, serialNumber) {
  const sn = String(serialNumber ?? "").trim();
  if (!sn) return null;
  const hit = await get(
    `SELECT p.name AS product_name
     FROM product_serials ps
     JOIN products p ON p.id = ps.product_id AND p.company_id = ps.company_id
     JOIN products self ON self.id = ? AND self.company_id = ?
     WHERE ps.company_id = ?
       AND UPPER(TRIM(ps.serial_number)) = UPPER(TRIM(?))
       AND ps.status IN ('EN_STOCK','PENDIENTE_INGRESO')
       AND IFNULL(p.category_id,-999999) IS NOT DISTINCT FROM IFNULL(self.category_id,-999999)
       AND IFNULL(p.brand_id,-888888) IS NOT DISTINCT FROM IFNULL(self.brand_id,-888888)
       AND IFNULL(TRIM(p.model),'') = IFNULL(TRIM(self.model),'')
     LIMIT 1;`,
    [targetProductId, companyId, companyId, sn]
  );
  return hit?.product_name || null;
}

async function migrateProductSerialsRemoveGlobalUnique() {
  const row = await get("SELECT sql FROM sqlite_master WHERE type='table' AND name='product_serials'");
  const sql = String(row?.sql || "");
  if (!sql.includes("UNIQUE(company_id, serial_number)")) return;
  await run(`
    CREATE TABLE IF NOT EXISTS product_serials_new (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      serial_number TEXT NOT NULL,
      purchase_id INTEGER,
      deposit_id INTEGER,
      sector_id INTEGER,
      status TEXT NOT NULL DEFAULT 'EN_STOCK',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      FOREIGN KEY(product_id) REFERENCES products(id),
      FOREIGN KEY(purchase_id) REFERENCES purchases(id),
      FOREIGN KEY(deposit_id) REFERENCES deposits(id),
      FOREIGN KEY(sector_id) REFERENCES sectors(id)
    );`);
  await run(`INSERT INTO product_serials_new SELECT * FROM product_serials;`);
  await run(`DROP TABLE product_serials;`);
  await run(`ALTER TABLE product_serials_new RENAME TO product_serials;`);
  await run(`CREATE INDEX IF NOT EXISTS idx_ps_company_serial ON product_serials(company_id, serial_number);`);
}

function requireAuth(req, res, next) {
  if (!req.session.user) {
    return res.redirect("/login");
  }
  return next();
}

function renderAppShell({
  title,
  subtitle = "",
  companyName,
  username,
  allowedModules = [],
  activeGroup = "",
  activeSection = "",
  body = "",
}) {
  const groups = getVisibleModuleGroups(allowedModules);
  const groupCards = groups
    .map((group) => {
      const activeClass = group.key === activeGroup ? "launcher-card active" : "launcher-card";
      return `<a class="${activeClass}" href="${group.path}">
        <strong>${group.label}</strong>
        <span>${group.subtitle}</span>
      </a>`;
    })
    .join("");

  const group = MODULE_GROUPS.find((item) => item.key === activeGroup);
  const visibleSections = group
    ? group.sections.filter((section) => allowedModules.includes(section.moduleKey))
    : [];
  const sectionLinks = visibleSections
    .map((section) => {
      const activeClass = section.moduleKey === activeSection ? "side-link active" : "side-link";
      return `<a class="${activeClass}" href="${section.path}">${section.label}</a>`;
    })
    .join("");

  return `
    <div class="app-shell">
      <aside class="side-panel">
        <div class="brand-block">
          <div class="brand-title">POST VENTA</div>
          <div class="brand-subtitle">Electrodomesticos</div>
        </div>
        <div class="side-section-title">${escapeHtml(group ? group.label : "Navegacion")}</div>
        <div class="side-links">${sectionLinks || "<div class='muted'>Sin apartados</div>"}</div>
      </aside>
      <main class="main-panel">
        <div class="topbar">
          <div>
            <h2>${escapeHtml(title)}</h2>
            <div class="muted">${escapeHtml(subtitle || "")}</div>
            <div class="muted">Empresa: <strong>${escapeHtml(companyName || "")}</strong> | Usuario: ${escapeHtml(
              username || ""
            )}</div>
          </div>
          <div class="topbar-actions">
            <button type="button" class="module-launcher-button" onclick="toggleModulePanel()">Abrir modulos</button>
            <form method="post" action="/logout"><button type="submit" class="danger-button">Salir</button></form>
          </div>
        </div>
        <div id="module-panel" class="module-panel">
          ${groupCards || "<p class='muted'>No tienes modulos habilitados.</p>"}
        </div>
        <section class="content-card">
          ${body}
        </section>
      </main>
    </div>
    <script>
      function toggleModulePanel() {
        var panel = document.getElementById("module-panel");
        if (!panel) return;
        panel.classList.toggle("open");
      }
      document.addEventListener("click", function(event) {
        var panel = document.getElementById("module-panel");
        var button = document.querySelector(".module-launcher-button");
        if (!panel || !button) return;
        if (!panel.contains(event.target) && !button.contains(event.target)) {
          panel.classList.remove("open");
        }
      });
    </script>`;
}

async function ensureSchema() {
  await run(`
    CREATE TABLE IF NOT EXISTS companies (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL DEFAULT 'ACT'
    );
  `);
  try {
    await run("ALTER TABLE companies ADD COLUMN ruc TEXT;");
  } catch {}

  await run(`
    CREATE TABLE IF NOT EXISTS workers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      first_name TEXT NOT NULL,
      last_name TEXT NOT NULL,
      document_type TEXT NOT NULL DEFAULT 'DNI',
      document_number TEXT NOT NULL,
      phone TEXT,
      email TEXT,
      status TEXT NOT NULL DEFAULT 'ACTIVO',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      UNIQUE(company_id, document_number)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL,
      password TEXT NOT NULL,
      company_id INTEGER NOT NULL,
      worker_id INTEGER,
      role TEXT NOT NULL DEFAULT 'agent',
      status TEXT NOT NULL DEFAULT 'ACTIVO',
      FOREIGN KEY(company_id) REFERENCES companies(id),
      FOREIGN KEY(worker_id) REFERENCES workers(id)
    );
  `);

  try {
    await run("ALTER TABLE users ADD COLUMN worker_id INTEGER;");
  } catch {
    // Columna existente
  }
  try {
    await run("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVO';");
  } catch {}

  await run(
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_unique ON users(username);"
  );
  await run(
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_worker_unique ON users(worker_id);"
  );

  await run(`
    CREATE TABLE IF NOT EXISTS profiles (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      description TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      UNIQUE(company_id, name)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS profile_modules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      profile_id INTEGER NOT NULL,
      module_key TEXT NOT NULL,
      can_access INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY(profile_id) REFERENCES profiles(id),
      UNIQUE(profile_id, module_key)
    );
  `);

  try {
    await run("ALTER TABLE users ADD COLUMN profile_id INTEGER;");
  } catch {
    // Columna existente
  }

  await run(`
    CREATE TABLE IF NOT EXISTS brands (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'ACTIVO',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      UNIQUE(company_id, name)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS suppliers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      contact_name TEXT,
      phone TEXT,
      email TEXT,
      status TEXT NOT NULL DEFAULT 'ACTIVO',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      UNIQUE(company_id, name)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS customers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      full_name TEXT NOT NULL,
      document_number TEXT,
      phone TEXT,
      email TEXT,
      address TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      UNIQUE(company_id, document_number)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      brand_id INTEGER,
      category_id INTEGER,
      supplier_id INTEGER,
      sku TEXT NOT NULL,
      barcode TEXT,
      name TEXT NOT NULL,
      category TEXT,
      model TEXT,
      current_stock INTEGER NOT NULL DEFAULT 0,
      reorder_level INTEGER NOT NULL DEFAULT 5,
      requires_serial INTEGER NOT NULL DEFAULT 0,
      image_path TEXT,
      status TEXT NOT NULL DEFAULT 'ACTIVO',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      FOREIGN KEY(brand_id) REFERENCES brands(id),
      FOREIGN KEY(category_id) REFERENCES categories(id),
      FOREIGN KEY(supplier_id) REFERENCES suppliers(id),
      UNIQUE(company_id, sku),
      UNIQUE(company_id, barcode)
    );
  `);

  try {
    await run("ALTER TABLE products ADD COLUMN category_id INTEGER;");
  } catch {}
  try {
    await run("ALTER TABLE products ADD COLUMN reorder_level INTEGER NOT NULL DEFAULT 5;");
  } catch {}
  try {
    await run("ALTER TABLE products ADD COLUMN requires_serial INTEGER NOT NULL DEFAULT 0;");
  } catch {}
  try {
    await run("ALTER TABLE products ADD COLUMN barcode TEXT;");
  } catch {}
  try {
    await run("ALTER TABLE products ADD COLUMN image_path TEXT;");
  } catch {}
  try {
    await run("ALTER TABLE products ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVO';");
  } catch {}

  await run(`
    CREATE TABLE IF NOT EXISTS categories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'ACTIVO',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      UNIQUE(company_id, name)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS stock_movements (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      movement_type TEXT NOT NULL,
      quantity INTEGER NOT NULL,
      note TEXT,
      purchase_id INTEGER,
      supplier_id INTEGER,
      deposit_id INTEGER,
      sector_id INTEGER,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      FOREIGN KEY(product_id) REFERENCES products(id)
    );
  `);
  try {
    await run("ALTER TABLE stock_movements ADD COLUMN purchase_id INTEGER;");
  } catch {}
  try {
    await run("ALTER TABLE stock_movements ADD COLUMN supplier_id INTEGER;");
  } catch {}
  try {
    await run("ALTER TABLE stock_movements ADD COLUMN deposit_id INTEGER;");
  } catch {}
  try {
    await run("ALTER TABLE stock_movements ADD COLUMN sector_id INTEGER;");
  } catch {}

  await run(`
    CREATE TABLE IF NOT EXISTS product_prices (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      price REAL NOT NULL,
      effective_date TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      note TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      FOREIGN KEY(product_id) REFERENCES products(id)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS approvers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'ACTIVO',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      FOREIGN KEY(user_id) REFERENCES users(id),
      UNIQUE(company_id, user_id)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS purchase_requests (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      request_code TEXT NOT NULL UNIQUE,
      requested_by_user_id INTEGER NOT NULL,
      requested_by_worker_id INTEGER,
      category_id INTEGER,
      requested_item TEXT NOT NULL,
      quantity INTEGER NOT NULL,
      note TEXT,
      status TEXT NOT NULL DEFAULT 'PENDIENTE',
      approved_by_user_id INTEGER,
      approved_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      FOREIGN KEY(requested_by_user_id) REFERENCES users(id),
      FOREIGN KEY(requested_by_worker_id) REFERENCES workers(id),
      FOREIGN KEY(category_id) REFERENCES categories(id),
      FOREIGN KEY(approved_by_user_id) REFERENCES users(id)
    );
  `);
  try {
    await run("ALTER TABLE purchase_requests ADD COLUMN category_id INTEGER;");
  } catch {}

  await run(`
    CREATE TABLE IF NOT EXISTS request_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      request_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      quantity INTEGER NOT NULL,
      note TEXT,
      FOREIGN KEY(request_id) REFERENCES purchase_requests(id),
      FOREIGN KEY(product_id) REFERENCES products(id)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS purchases (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      voucher_code TEXT NOT NULL UNIQUE,
      supplier_id INTEGER,
      request_id INTEGER,
      product_id INTEGER NOT NULL,
      quantity INTEGER NOT NULL,
      unit_price REAL NOT NULL,
      total REAL NOT NULL,
      status TEXT NOT NULL DEFAULT 'PENDIENTE_APROBACION',
      approved_by_user_id INTEGER,
      approved_at TEXT,
      created_by_user_id INTEGER NOT NULL,
      receipt_code TEXT,
      stock_received_at TEXT,
      received_by_user_id INTEGER,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      FOREIGN KEY(supplier_id) REFERENCES suppliers(id),
      FOREIGN KEY(request_id) REFERENCES purchase_requests(id),
      FOREIGN KEY(product_id) REFERENCES products(id),
      FOREIGN KEY(approved_by_user_id) REFERENCES users(id),
      FOREIGN KEY(created_by_user_id) REFERENCES users(id),
      FOREIGN KEY(received_by_user_id) REFERENCES users(id)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS purchase_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      purchase_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      quantity INTEGER NOT NULL,
      unit_price REAL NOT NULL,
      total REAL NOT NULL,
      note TEXT,
      FOREIGN KEY(purchase_id) REFERENCES purchases(id),
      FOREIGN KEY(product_id) REFERENCES products(id)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS deposits (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'ACTIVO',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      UNIQUE(company_id, name)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS sectors (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      deposit_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'ACTIVO',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      FOREIGN KEY(deposit_id) REFERENCES deposits(id),
      UNIQUE(company_id, deposit_id, name)
    );
  `);

  await run(`
    CREATE TABLE IF NOT EXISTS product_serials (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      serial_number TEXT NOT NULL,
      purchase_id INTEGER,
      deposit_id INTEGER,
      sector_id INTEGER,
      status TEXT NOT NULL DEFAULT 'EN_STOCK',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      FOREIGN KEY(product_id) REFERENCES products(id),
      FOREIGN KEY(purchase_id) REFERENCES purchases(id),
      FOREIGN KEY(deposit_id) REFERENCES deposits(id),
      FOREIGN KEY(sector_id) REFERENCES sectors(id)
    );
  `);
  await migrateProductSerialsRemoveGlobalUnique();

  await run(`
    CREATE TABLE IF NOT EXISTS inventory_locations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      deposit_id INTEGER NOT NULL,
      sector_id INTEGER NOT NULL,
      quantity INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id),
      FOREIGN KEY(product_id) REFERENCES products(id),
      FOREIGN KEY(deposit_id) REFERENCES deposits(id),
      FOREIGN KEY(sector_id) REFERENCES sectors(id),
      UNIQUE(company_id, product_id, deposit_id, sector_id)
    );
  `);

  try {
    await run("ALTER TABLE purchases ADD COLUMN deposit_id INTEGER;");
  } catch {}
  try {
    await run("ALTER TABLE purchases ADD COLUMN sector_id INTEGER;");
  } catch {}
  try {
    await run("ALTER TABLE purchases ADD COLUMN ingress_doc_type TEXT;");
  } catch {}
  try {
    await run("ALTER TABLE purchases ADD COLUMN ingress_doc_series TEXT;");
  } catch {}
  try {
    await run("ALTER TABLE purchases ADD COLUMN ingress_doc_pdf TEXT;");
  } catch {}
  try {
    await run("ALTER TABLE suppliers ADD COLUMN ruc TEXT;");
  } catch {}
  try {
    await run("ALTER TABLE purchases ADD COLUMN ingress_rejection_note TEXT;");
  } catch {}
  try {
    await run("ALTER TABLE purchases ADD COLUMN ingress_rejected_at TEXT;");
  } catch {}
  try {
    await run("ALTER TABLE purchases ADD COLUMN ingress_rejected_by_user_id INTEGER;");
  } catch {}
}

const MODULES = [
  { key: "dashboard", label: "Inicio" },
  { key: "workers", label: "Trabajadores" },
  { key: "users", label: "Usuarios" },
  { key: "profiles", label: "Perfiles" },
  { key: "brands", label: "Marcas" },
  { key: "products", label: "Productos" },
  { key: "stock", label: "Stock" },
  { key: "suppliers", label: "Proveedores" },
  // customers removed (not used)
  { key: "categories", label: "Categorias" },
  { key: "requests", label: "Solicitudes" },
  { key: "purchases", label: "Compras" },
  { key: "approvers", label: "Aprobadores" },
  { key: "approvals_requests", label: "Aprobacion Solicitudes" },
  { key: "approvals_purchases", label: "Aprobacion Compras" },
  { key: "deposits", label: "Depositos" },
  { key: "sectors", label: "Sectores" },
  { key: "ingresses", label: "Ingresos" },
  { key: "approvals_ingresses", label: "Aprobacion Ingresos" },
  { key: "reports", label: "Reportes" },
  { key: "kardex", label: "Kardex" },
];

const MODULE_GROUPS = [
  {
    key: "home",
    label: "Inicio",
    subtitle: "Resumen general",
    path: "/dashboard",
    sections: [{ moduleKey: "dashboard", label: "Dashboard", path: "/dashboard" }],
  },
  {
    key: "user-management",
    label: "Gestion de usuarios",
    subtitle: "Usuarios y perfiles",
    path: "/modules/users",
    sections: [
      { moduleKey: "users", label: "Usuarios", path: "/users" },
      { moduleKey: "profiles", label: "Perfiles", path: "/profiles" },
      { moduleKey: "approvers", label: "Aprobadores", path: "/approvers" },
    ],
  },
  {
    key: "human-talent",
    label: "Talento humano",
    subtitle: "Trabajadores",
    path: "/modules/talent",
    sections: [{ moduleKey: "workers", label: "Trabajadores", path: "/workers" }],
  },
  {
    key: "logistics",
    label: "Logistica",
    subtitle: "Catalogo, stock y relacion comercial",
    path: "/modules/logistics",
    sections: [
      { moduleKey: "products", label: "Productos", path: "/products" },
      { moduleKey: "stock", label: "Stock", path: "/stock" },
      { moduleKey: "categories", label: "Categorias", path: "/categories" },
      { moduleKey: "brands", label: "Marcas", path: "/brands" },
      { moduleKey: "suppliers", label: "Proveedores", path: "/suppliers" },
      { moduleKey: "requests", label: "Solicitudes", path: "/requests" },
      { moduleKey: "purchases", label: "Compras", path: "/purchases" },
      { moduleKey: "approvals_requests", label: "Aprob. Solicitudes", path: "/approvals/requests" },
      { moduleKey: "approvals_purchases", label: "Aprob. Compras", path: "/approvals/purchases" },
      { moduleKey: "deposits", label: "Depositos", path: "/deposits" },
      { moduleKey: "sectors", label: "Sectores", path: "/sectors" },
      { moduleKey: "ingresses", label: "Ingresos", path: "/ingresses" },
      { moduleKey: "approvals_ingresses", label: "Aprob. Ingresos", path: "/approvals/ingresses" },
      { moduleKey: "reports", label: "Reportes", path: "/reports" },
      { moduleKey: "kardex", label: "Kardex", path: "/kardex" },
    ],
  },
];

function getVisibleModuleGroups(allowedModules = []) {
  return MODULE_GROUPS.filter((group) =>
    group.sections.some((section) => allowedModules.includes(section.moduleKey))
  );
}

function getDefaultLandingPath(allowedModules = []) {
  if (allowedModules.includes("dashboard")) return "/dashboard";
  const firstGroup = getVisibleModuleGroups(allowedModules)[0];
  return firstGroup ? firstGroup.path : "/login";
}

const PRIMARY_COMPANY_NAME = "ALLIN GROUP - JAVIER PRADO S.A.";

/** Crea usuario admin / admin_<id> por empresa si falta (misma contrasena demo admin123). */
async function ensureBootstrapDemoUsers() {
  const primary = await get("SELECT id FROM companies WHERE name = ?;", [PRIMARY_COMPANY_NAME]);
  const companies = await all("SELECT id FROM companies WHERE status='ACT' ORDER BY id;");
  for (const c of companies) {
    await ensureCompanyProfiles(c.id);
    const adminProfile = await get(
      "SELECT id FROM profiles WHERE company_id = ? AND name = 'Administrador';",
      [c.id]
    );
    if (!adminProfile) continue;
    const loginUsername = primary && c.id === primary.id ? "admin" : `admin_${c.id}`;
    await run(
      "INSERT OR IGNORE INTO users(username, password, company_id, role, profile_id, status) VALUES(?, 'admin123', ?, 'admin', ?, 'ACTIVO');",
      [loginUsername, c.id, adminProfile.id]
    );
  }
  await run(
    `UPDATE users SET profile_id = (
       SELECT p.id FROM profiles p WHERE p.company_id = users.company_id AND p.name = 'Administrador' LIMIT 1
     )
     WHERE role = 'admin' AND profile_id IS NULL;`
  );
}

async function ensureCompanyProfiles(companyId) {
  await run("INSERT OR IGNORE INTO profiles(company_id, name, description) VALUES (?, ?, ?);", [
    companyId,
    "Administrador",
    "Acceso total a todos los modulos",
  ]);
  await run("INSERT OR IGNORE INTO profiles(company_id, name, description) VALUES (?, ?, ?);", [
    companyId,
    "Supervisor",
    "Acceso a operaciones y lectura de usuarios",
  ]);
  await run("INSERT OR IGNORE INTO profiles(company_id, name, description) VALUES (?, ?, ?);", [
    companyId,
    "Operador",
    "Acceso solo a dashboard",
  ]);

  const admin = await get("SELECT id FROM profiles WHERE company_id = ? AND name = 'Administrador';", [companyId]);
  const supervisor = await get("SELECT id FROM profiles WHERE company_id = ? AND name = 'Supervisor';", [companyId]);
  const operator = await get("SELECT id FROM profiles WHERE company_id = ? AND name = 'Operador';", [companyId]);

  for (const moduleDef of MODULES) {
    if (admin?.id) {
      await run(
        "INSERT OR IGNORE INTO profile_modules(profile_id, module_key, can_access) VALUES (?, ?, 1);",
        [admin.id, moduleDef.key]
      );
    }
    if (supervisor?.id) {
      await run(
        "INSERT OR IGNORE INTO profile_modules(profile_id, module_key, can_access) VALUES (?, ?, ?);",
        [supervisor.id, moduleDef.key, moduleDef.key === "profiles" || moduleDef.key === "approvers" ? 0 : 1]
      );
    }
    if (operator?.id) {
      await run(
        "INSERT OR IGNORE INTO profile_modules(profile_id, module_key, can_access) VALUES (?, ?, ?);",
        [operator.id, moduleDef.key, moduleDef.key === "dashboard" ? 1 : 0]
      );
    }
  }
}

async function getAllowedModules(userId) {
  const rows = await all(
    `SELECT pm.module_key
     FROM users u
     JOIN profile_modules pm ON pm.profile_id = u.profile_id
     WHERE u.id = ? AND pm.can_access = 1;`,
    [userId]
  );
  return rows.map((row) => row.module_key);
}

async function isApprover(companyId, userId) {
  const approver = await get(
    "SELECT id FROM approvers WHERE company_id = ? AND user_id = ? AND status = 'ACTIVO';",
    [companyId, userId]
  );
  return Boolean(approver);
}

function csvEscape(value) {
  const raw = value == null ? "" : String(value);
  return `"${raw.replaceAll('"', '""')}"`;
}

function sendCsv(res, filename, columns, rows) {
  const header = columns.map((col) => csvEscape(col.label)).join(",");
  const body = rows
    .map((row) => columns.map((col) => csvEscape(row[col.key])).join(","))
    .join("\n");
  const content = `${header}\n${body}\n`;
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
  res.send(content);
}

function toArray(value) {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  return [value];
}

/** Correlativo por empresa; incluye company_id porque request_code y voucher_code son UNIQUE globales en SQLite. */
async function nextPurchaseRequestCode(companyId) {
  const seq = await get(
    "SELECT IFNULL(MAX(id),0)+1 AS next_id FROM purchase_requests WHERE company_id = ?;",
    [companyId]
  );
  const co = String(companyId).padStart(3, "0");
  return `SOL-${co}-${String(Number(seq?.next_id) || 1).padStart(5, "0")}`;
}

async function nextPurchaseVoucherCode(companyId) {
  const seq = await get("SELECT IFNULL(MAX(id),0)+1 AS next_id FROM purchases WHERE company_id=?;", [companyId]);
  const co = String(companyId).padStart(3, "0");
  return `COMP-${co}-${String(Number(seq?.next_id) || 1).padStart(6, "0")}`;
}

async function nextIngressReceiptCode(companyId) {
  const co = String(companyId).padStart(3, "0");
  const prefix = `ING-${co}-`;
  const rows = await all("SELECT receipt_code FROM purchases WHERE company_id=? AND receipt_code LIKE ?;", [
    companyId,
    `${prefix}%`,
  ]);
  let maxSeq = 0;
  for (const r of rows) {
    const m = String(r.receipt_code || "").match(new RegExp(`^ING-${co}-(\\d+)$`));
    if (m) maxSeq = Math.max(maxSeq, parseInt(m[1], 10));
  }
  return `${prefix}${String(maxSeq + 1).padStart(5, "0")}`;
}

function safePdfFilename(name) {
  const base = String(name || "documento.pdf").replace(/[\r\n"]/g, "_");
  return base.replace(/[^\w.\-]+/g, "_").slice(0, 180) || "documento.pdf";
}

/** Genera el PDF en memoria y responde una sola vez (evita ERR_HTTP_HEADERS_SENT si el cliente corta o hay error al hacer pipe directo). */
function sendSimplePdf(res, filename, title, lines) {
  const safeName = safePdfFilename(filename);
  const doc = new PDFDocument({ margin: 40 });
  const chunks = [];
  let finalized = false;
  const fail = (err) => {
    if (finalized) return;
    finalized = true;
    console.error("sendSimplePdf:", err);
    if (!res.headersSent) {
      res.status(500).type("text/plain").send("No se pudo generar el PDF.");
    }
  };
  doc.on("data", (c) => chunks.push(c));
  doc.on("error", fail);
  doc.on("end", () => {
    if (finalized || res.headersSent || res.writableEnded) return;
    finalized = true;
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${safeName}"`);
    res.send(Buffer.concat(chunks));
  });
  try {
    doc.fontSize(16).text(title);
    doc.moveDown();
    for (const line of lines) {
      doc.fontSize(11).text(line);
    }
    doc.end();
  } catch (err) {
    fail(err);
  }
}

function sendVoucherPdf(res, filename, config) {
  const safeName = safePdfFilename(filename);
  const M = 40;
  const doc = new PDFDocument({ margin: M, size: "A4" });
  const chunks = [];
  let finalized = false;
  const fail = (err) => {
    if (finalized) return;
    finalized = true;
    console.error("sendVoucherPdf:", err);
    if (!res.headersSent) {
      res.status(500).type("text/plain").send("No se pudo generar el PDF del comprobante.");
    }
  };
  doc.on("data", (c) => chunks.push(c));
  doc.on("error", fail);
  doc.on("end", () => {
    if (finalized || res.headersSent || res.writableEnded) return;
    finalized = true;
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${safeName}"`);
    res.send(Buffer.concat(chunks));
  });

  const pageBottom = doc.page.height - M;
  const contentW = doc.page.width - 2 * M;
  const showPrices = config.showPrices !== false;

  function newPage() {
    doc.addPage();
    return M + 8;
  }

  try {
    let y = M;

    doc.font("Helvetica-Bold").fontSize(17).fillColor("#0f172a");
    const titleText = config.title || "Documento";
    const titleBlockH = Math.max(doc.heightOfString(titleText, { width: contentW - 92, lineGap: 2 }), 16);
    doc.text(titleText, M, y, { width: contentW - 92, lineGap: 2 });
    doc.font("Helvetica").fontSize(9).fillColor("#64748b").text("Original · Pag. 1", doc.page.width - M - 96, y, {
      width: 96,
      align: "right",
      lineGap: 2,
    });
    y += titleBlockH + 14;

    const colGap = 12;
    const colW = (contentW - colGap) / 2;

    const leftTexts = [];
    doc.font("Helvetica-Bold").fontSize(11);
    leftTexts.push({ bold: true, size: 11, text: String(config.companyName || "Empresa") });
    const cr = config.companyRuc != null ? String(config.companyRuc).trim() : "";
    if (cr && cr !== "-") leftTexts.push({ bold: false, size: 9, text: `RUC empresa: ${cr}` });
    leftTexts.push({ bold: false, size: 9, text: String(config.companyInfo || "Post Venta — documento oficial") });

    let hLeft = 0;
    leftTexts.forEach((seg) => {
      doc.font(seg.bold ? "Helvetica-Bold" : "Helvetica").fontSize(seg.size);
      hLeft += doc.heightOfString(seg.text, { width: colW - 24, lineGap: 2 }) + 5;
    });

    const rs = formatDocStatus(config.status);
    const rightBlock = [`Comprobante: ${config.code || "-"}`, `Fecha: ${config.date || "-"}`, `Estado: ${rs}`];
    doc.font("Helvetica").fontSize(10);
    let hRight = 0;
    rightBlock.forEach((line) => {
      hRight += doc.heightOfString(line, { width: colW - 24, lineGap: 3 }) + 5;
    });

    const boxPad = 14;
    const headerH = Math.max(hLeft, hRight, 52) + boxPad;
    const boxTop = y;

    doc.save();
    doc.rect(M, boxTop, colW, headerH).fill("#f8fafc");
    doc.rect(M + colW + colGap, boxTop, colW, headerH).fill("#eff6ff");
    doc.restore();
    doc.rect(M, boxTop, colW, headerH).stroke("#e2e8f0");
    doc.rect(M + colW + colGap, boxTop, colW, headerH).stroke("#cbd5e1");

    let ly = boxTop + boxPad / 2;
    leftTexts.forEach((seg) => {
      doc.font(seg.bold ? "Helvetica-Bold" : "Helvetica").fontSize(seg.size).fillColor("#0f172a");
      const hh = doc.heightOfString(seg.text, { width: colW - 24, lineGap: 2 });
      doc.text(seg.text, M + 12, ly, { width: colW - 24, lineGap: 2 });
      ly += hh + 5;
    });

    let ry = boxTop + boxPad / 2;
    doc.font("Helvetica").fontSize(10).fillColor("#0f172a");
    rightBlock.forEach((line) => {
      const hh = doc.heightOfString(line, { width: colW - 24, lineGap: 3 });
      doc.text(line, M + colW + colGap + 12, ry, { width: colW - 24, lineGap: 3 });
      ry += hh + 5;
    });

    y = boxTop + headerH + 18;

    const metaLines = config.metaLines || [];
    if (metaLines.length) {
      doc.font("Helvetica-Bold").fontSize(10).fillColor("#475569").text("Datos", M, y);
      y += 13;
      doc.font("Helvetica").fontSize(9).fillColor("#334155");
      let mbh = 12;
      metaLines.forEach((ln) => {
        mbh += doc.heightOfString(String(ln), { width: contentW - 28, lineGap: 3 }) + 6;
      });
      doc.save();
      doc.rect(M, y, contentW, mbh).fill("#ffffff");
      doc.restore();
      doc.rect(M, y, contentW, mbh).stroke("#e5e7eb");

      let my = y + 10;
      metaLines.forEach((ln) => {
        doc.font("Helvetica").fontSize(9).text(String(ln), M + 14, my, {
          width: contentW - 28,
          lineGap: 3,
        });
        my += doc.heightOfString(String(ln), { width: contentW - 28, lineGap: 3 }) + 6;
      });
      y += mbh + 14;
    }

    const items = config.items || [];
    const showDetail =
      config.showProductDetails !== false &&
      items.some((i) => i.brandName || i.categoryName || i.modelName);

    let xName = M;
    let nameW;
    let xBrand;
    let xCat;
    let xModel;
    let xQty;
    let xPu;
    let xTot;

    if (showDetail && showPrices) {
      nameW = 112;
      const brandW = 62;
      const catW = 66;
      const modelW = 56;
      const qtyW = 34;
      const puW = 50;
      const totW = 52;
      xBrand = xName + nameW + 6;
      xCat = xBrand + brandW + 6;
      xModel = xCat + catW + 6;
      xQty = xModel + modelW + 6;
      xPu = xQty + qtyW + 6;
      xTot = xPu + puW + 4;
    } else if (showDetail && !showPrices) {
      nameW = 140;
      const brandW = 72;
      const catW = 76;
      const modelW = 64;
      const qtyW = 36;
      xBrand = xName + nameW + 8;
      xCat = xBrand + brandW + 8;
      xModel = xCat + catW + 8;
      xQty = xModel + modelW + 10;
      xPu = 0;
      xTot = 0;
    } else if (showPrices) {
      nameW = 268;
      xQty = xName + nameW + 12;
      xPu = xQty + 44;
      xTot = xPu + 58;
    } else {
      nameW = 350;
      xQty = xName + nameW + 14;
      xPu = 0;
      xTot = 0;
    }

    doc.font("Helvetica-Bold").fontSize(9).fillColor("#475569");
    doc.text("Producto", xName, y);
    if (showDetail) {
      doc.text("Marca", xBrand, y);
      doc.text("Categoria", xCat, y);
      doc.text("Modelo", xModel, y);
    }
    doc.text("Cant.", xQty, y);
    if (showPrices) {
      doc.text("P.Unit", xPu, y);
      doc.text("Total", xTot, y);
    }
    y += 12;
    doc.moveTo(M, y).lineTo(M + contentW, y).stroke("#cbd5e1");
    y += 8;

    doc.font("Helvetica").fontSize(9).fillColor("#0f172a");

    items.forEach((item) => {
      const nm = String(item.name || "-");
      let rowH = doc.heightOfString(nm, { width: nameW, lineGap: 2 });
      if (showDetail) {
        rowH = Math.max(
          rowH,
          doc.heightOfString(String(item.brandName || "-"), { width: 62, lineGap: 1 }),
          doc.heightOfString(String(item.categoryName || "-"), { width: 66, lineGap: 1 }),
          doc.heightOfString(String(item.modelName || "-"), { width: 56, lineGap: 1 })
        );
      }
      rowH = Math.max(rowH, 12);
      if (y + rowH + 40 > pageBottom) y = newPage();

      doc.font("Helvetica").fontSize(9).text(nm, xName, y, { width: nameW, lineGap: 2 });
      if (showDetail) {
        const bw = showPrices ? 62 : 72;
        const cw = showPrices ? 66 : 76;
        const mw = showPrices ? 56 : 64;
        doc.text(String(item.brandName || "-"), xBrand, y, { width: bw, lineGap: 1 });
        doc.text(String(item.categoryName || "-"), xCat, y, { width: cw, lineGap: 1 });
        doc.text(String(item.modelName || "-"), xModel, y, { width: mw, lineGap: 1 });
      }
      doc.text(String(item.quantity ?? "-"), xQty, y);
      if (showPrices) {
        doc.text(item.unitPrice != null ? Number(item.unitPrice).toFixed(2) : "-", xPu, y);
        doc.text(item.total != null ? Number(item.total).toFixed(2) : "-", xTot, y);
      }
      y += rowH + 6;

      const serialLines = item.serialLines || [];
      if (serialLines.length) {
        const raw = serialLines.map((s) => String(s).replace(/^\d+:\s*/, ""));
        const compact = raw.join("  ·  ");
        if (y + 24 > pageBottom) y = newPage();
        doc.font("Helvetica-Bold").fontSize(8).fillColor("#0369a1").text("Series:", M + 6, y);
        y += 10;
        doc.font("Helvetica").fontSize(8).fillColor("#334155").text(compact, M + 10, y, {
          width: contentW - 20,
          lineGap: 2,
        });
        y += doc.heightOfString(compact, { width: contentW - 20, lineGap: 2 }) + 10;
      }

      doc.moveTo(M + 6, y).lineTo(M + contentW - 6, y).stroke("#f1f5f9");
      y += 8;
    });

    if (showPrices) {
      const grandTotal =
        config.total != null
          ? Number(config.total)
          : items.reduce((acc, i) => acc + Number(i.total || 0), 0);
      if (y + 46 > pageBottom) y = newPage();
      const tw = 200;
      const tx = M + contentW - tw;
      doc.rect(tx, y, tw, 34).fill("#f8fafc").stroke("#e2e8f0");
      doc.font("Helvetica-Bold").fontSize(11).fillColor("#0f172a").text(`Total: ${grandTotal.toFixed(2)}`, tx + 10, y + 11, {
        width: tw - 20,
      });
      y += 42;
    }

    if (y + 28 > pageBottom) y = newPage();
    doc.font("Helvetica").fontSize(8).fillColor("#64748b").text(
      config.footer || "Documento generado por sistema Post Venta.",
      M,
      y,
      { width: contentW, lineGap: 2 }
    );

    doc.end();
  } catch (err) {
    fail(err);
  }
}

function requireModule(moduleKey) {
  return (req, res, next) => {
    const allowedModules = req.session.user?.allowedModules || [];
    if (!allowedModules.includes(moduleKey)) {
      return res.status(403).send(
        renderLayout(
          "Acceso denegado",
          `<div class="container"><h2>Sin permisos</h2><p>No tienes acceso al modulo solicitado.</p><p><a href="/dashboard">Volver al inicio</a></p></div>`
        )
      );
    }
    return next();
  };
}

function requireAnyModule(moduleKeys) {
  return (req, res, next) => {
    const allowedModules = req.session.user?.allowedModules || [];
    const hasAccess = moduleKeys.some((key) => allowedModules.includes(key));
    if (!hasAccess) {
      return res.status(403).send(
        renderLayout(
          "Acceso denegado",
          `<div class="container"><h2>Sin permisos</h2><p>No tienes acceso al modulo solicitado.</p><p><a href="/dashboard">Volver al inicio</a></p></div>`
        )
      );
    }
    return next();
  };
}

function renderLayout(title, content) {
  return `<!doctype html>
  <html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${escapeHtml(title)}</title>
    <style>
      body { margin: 0; font-family: "Segoe UI", Tahoma, Arial, sans-serif; background: #eef2f7; color: #0f172a; }
      h1,h2,h3 { margin: 0 0 14px 0; color: #0f172a; }
      .muted { color: #64748b; font-size: 13px; }
      .app-shell { display: grid; grid-template-columns: 260px 1fr; min-height: 100vh; }
      .side-panel { background: linear-gradient(180deg, #0f172a 0%, #111827 100%); color: #dbeafe; padding: 22px 16px; border-right: 1px solid #1f2937; }
      .brand-block { border-bottom: 1px solid #1f2d46; padding-bottom: 14px; margin-bottom: 16px; }
      .brand-title { font-weight: 800; font-size: 20px; letter-spacing: .5px; color: #f8fafc; }
      .brand-subtitle { color: #93c5fd; font-size: 13px; }
      .side-section-title { font-size: 12px; text-transform: uppercase; letter-spacing: .7px; color: #93c5fd; margin-bottom: 10px; }
      .side-links { display: flex; flex-direction: column; gap: 8px; }
      .side-link { text-decoration: none; color: #dbeafe; background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 10px 12px; font-weight: 600; }
      .side-link:hover { background: #334155; }
      .side-link.active { background: #2563eb; border-color: #3b82f6; color: #fff; }
      .main-panel { padding: 26px; }
      .topbar { display:flex; justify-content:space-between; align-items:flex-start; gap: 14px; margin-bottom: 12px; }
      .topbar-actions { display:flex; gap: 10px; align-items: center; }
      .content-card { background: #fff; border: 1px solid #dbe3ee; border-radius: 14px; box-shadow: 0 8px 24px rgba(15, 23, 42, .06); padding: 20px; }
      .container { max-width: 900px; margin: 40px auto; background: #fff; border: 1px solid #dbe3ee; border-radius: 14px; padding: 24px; }
      .row { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 18px 0; }
      .card { background: linear-gradient(180deg, #f8fbff 0%, #eff6ff 100%); border: 1px solid #dbeafe; border-radius: 10px; padding: 14px; }
      .card strong { font-size: 28px; color: #1d4ed8; display: block; margin-top: 6px; }
      .form-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 12px; margin: 16px 0; }
      input, select, textarea, button { width: 100%; padding: 10px 12px; border-radius: 8px; border: 1px solid #c7d0db; box-sizing: border-box; }
      input:focus, select:focus, textarea:focus { border-color: #3b82f6; outline: 2px solid #bfdbfe; }
      button { background: #16a34a; color: #fff; border: none; cursor: pointer; font-weight: 700; }
      button:hover { background: #15803d; }
      .danger-button { width: auto; background: #dc2626; padding: 10px 14px; }
      .danger-button:hover { background: #b91c1c; }
      .btn-compact { width: auto; display: inline-block; padding: 8px 12px; font-size: 12px; border-radius: 7px; }
      .action-cell { vertical-align: top; text-align: right; min-width: 200px; }
      .approval-actions { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; justify-content: flex-end; }
      .approval-actions form { margin: 0; display: inline-flex; width: auto; }
      .approval-actions button { width: auto; min-width: 96px; padding: 10px 14px; font-size: 13px; }
      .approval-actions .badge { text-decoration: none; }
      .ingress-reject-details { width: 100%; max-width: 280px; margin-left: auto; text-align: left; }
      .ingress-reject-details > summary {
        cursor: pointer; list-style: none; font-weight: 700; font-size: 13px; color: #b91c1c; padding: 8px 12px;
        border: 1px solid #fecaca; border-radius: 8px; background: #fff; width: auto; display: inline-block;
      }
      .ingress-reject-details > summary::-webkit-details-marker { display: none; }
      .reject-inline { margin-top: 10px; padding: 12px; border: 1px solid #e5e7eb; border-radius: 10px; background: #fef2f2; display: flex; flex-direction: column; gap: 8px; }
      .reject-inline textarea { min-height: 76px; width: 100%; }
      .action-row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin:8px 0; }
      .module-launcher-button { width: auto; background: #0f172a; padding: 10px 14px; }
      .module-launcher-button:hover { background: #1e293b; }
      .module-panel { display: none; background: #0b1633; border-radius: 12px; padding: 12px; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin: 0 0 14px 0; }
      .module-panel.open { display: grid; }
      .launcher-card { text-decoration: none; background: #13224a; color: #f8fafc; border: 1px solid #213b7a; border-radius: 10px; padding: 12px; display: flex; flex-direction: column; gap: 4px; }
      .launcher-card:hover { background: #1e3a8a; }
      .launcher-card.active { border-color: #60a5fa; box-shadow: 0 0 0 1px #60a5fa inset; }
      .launcher-card span { color: #cbd5e1; font-size: 12px; }
      table { width: 100%; border-collapse: collapse; margin-top: 15px; }
      th { background: #f8fafc; color: #334155; font-weight: 700; font-size: 12px; text-transform: uppercase; letter-spacing: .3px; }
      th, td { border-bottom: 1px solid #e5e7eb; text-align: left; padding: 11px; font-size: 14px; }
      .badge { display:inline-block; padding: 4px 8px; border-radius: 999px; background: #dbeafe; color: #1e40af; font-size: 12px; font-weight: 700; }
      .login-wrap { min-height: 100vh; display: grid; place-items: center; padding: 20px; }
      .login-box { max-width: 450px; width: 100%; background: #fff; border-radius: 14px; box-shadow: 0 16px 38px rgba(15, 23, 42, .14); padding: 24px; border: 1px solid #e2e8f0; }
      .login-title { font-size: 28px; margin-bottom: 5px; }
      .error { color: #b91c1c; margin-bottom: 10px; }
      .ok { color: #166534; margin-bottom: 10px; }
      @media (max-width: 980px) {
        .app-shell { grid-template-columns: 1fr; }
        .side-panel { border-right: 0; border-bottom: 1px solid #1f2937; }
        .main-panel { padding: 16px; }
        .topbar { flex-direction: column; }
      }
    </style>
  </head>
  <body>${content}</body>
  </html>`;
}

app.get("/", (req, res) => {
  if (!req.session.user) return res.redirect("/login");
  const modules = req.session.user.allowedModules || [];
  return res.redirect(getDefaultLandingPath(modules));
});

app.get("/login", async (req, res) => {
  const companies = await all("SELECT id, name FROM companies WHERE status='ACT' ORDER BY name;");
  const companyOptions = companies
    .map((c) => `<option value="${c.id}">${escapeHtml(c.name)}</option>`)
    .join("");
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";

  const html = `
    <div class="login-wrap">
      <div class="login-box">
        <h2 class="login-title">Post Venta</h2>
        <p class="muted">Plataforma corporativa multiempresa para electrodomesticos</p>
        ${error}
        <form method="post" action="/login">
          <label>Usuario</label>
          <input name="username" required />
          <label>Contrasena</label>
          <input type="password" name="password" required />
          <label>Empresa</label>
          <select name="companyId" required>
            ${companyOptions}
          </select>
          <div style="margin-top:14px;">
            <button type="submit">Iniciar sesion</button>
          </div>
        </form>
        <p class="muted" style="margin-top:12px">Empresa principal: usuario <strong>admin</strong>, contrasena <strong>admin123</strong>. Otras empresas: usuario <strong>admin_</strong> mas el ID de empresa (ej. <strong>admin_2</strong>), misma contrasena.</p>
      </div>
    </div>`;
  res.send(renderLayout("Login", html));
});

app.post("/login", async (req, res) => {
  await reloadFromDisk();
  const username = String(req.body.username ?? "").trim();
  const password = String(req.body.password ?? "").trim();
  const companyId = Number(req.body.companyId);
  if (!username || !password || !Number.isInteger(companyId) || companyId < 1) {
    return res.redirect("/login?error=Credenciales+o+empresa+incorrecta");
  }

  const nCompanies = Number((await get("SELECT COUNT(*) AS n FROM companies WHERE status='ACT';"))?.n || 0);
  const nAdmins = Number((await get("SELECT COUNT(*) AS n FROM users WHERE role='admin' AND status='ACTIVO';"))?.n || 0);
  if (nCompanies > 0 && nAdmins < nCompanies) {
    await ensureBootstrapDemoUsers();
  }

  await ensureCompanyProfiles(companyId);

  const defaultProfile = await get(
    "SELECT id FROM profiles WHERE company_id = ? AND name = 'Administrador';",
    [companyId]
  );
  await run(
    "UPDATE users SET profile_id = ? WHERE company_id = ? AND profile_id IS NULL AND role = 'admin';",
    [defaultProfile?.id || null, companyId]
  );

  const user = await get(
    "SELECT id, username, role, company_id, profile_id, worker_id FROM users WHERE username = ? AND password = ? AND company_id = ? AND status = 'ACTIVO';",
    [username, password, companyId]
  );

  if (!user) {
    return res.redirect("/login?error=Credenciales+o+empresa+incorrecta");
  }

  const company = await get(
    "SELECT id, name, IFNULL(TRIM(ruc),'') AS ruc FROM companies WHERE id = ?;",
    [user.company_id]
  );
  if (!company) {
    return res.redirect("/login?error=Credenciales+o+empresa+incorrecta");
  }
  let allowedModules = [];
  if (user.profile_id != null) {
    allowedModules = await getAllowedModules(user.id);
  } else if (user.role === "admin") {
    allowedModules = MODULES.map((m) => m.key);
  } else {
    allowedModules = ["dashboard"];
  }

  req.session.user = {
    id: user.id,
    username: user.username,
    role: user.role,
    companyId: user.company_id,
    companyName: company.name,
    companyRuc: company.ruc || "",
    profileId: user.profile_id,
    workerId: user.worker_id,
    allowedModules,
  };
  return res.redirect(getDefaultLandingPath(allowedModules));
});

app.post("/logout", (req, res) => {
  req.session.destroy(() => res.redirect("/login"));
});

app.get("/modules/users", requireAuth, requireAnyModule(["users", "profiles", "approvers"]), async (req, res) => {
  const { companyName, username, allowedModules } = req.session.user;
  const html = renderAppShell({
    title: "Gestion de usuarios",
    subtitle: "Administra usuarios y perfiles",
    companyName,
    username,
    allowedModules,
    activeGroup: "user-management",
    activeSection: "",
    body: `<p class="muted">Selecciona un apartado del menu lateral para continuar.</p>`,
  });
  res.send(renderLayout("Gestion de usuarios", html));
});

app.get("/modules/talent", requireAuth, requireAnyModule(["workers"]), async (req, res) => {
  const { companyName, username, allowedModules } = req.session.user;
  const html = renderAppShell({
    title: "Talento humano",
    subtitle: "Gestion del personal por empresa",
    companyName,
    username,
    allowedModules,
    activeGroup: "human-talent",
    activeSection: "",
    body: `<p class="muted">Selecciona un apartado del menu lateral para continuar.</p>`,
  });
  res.send(renderLayout("Talento humano", html));
});

app.get(
  "/modules/logistics",
  requireAuth,
  requireAnyModule([
    "products",
    "stock",
    "brands",
    "suppliers",
    "categories",
    "requests",
    "purchases",
    "approvals_requests",
    "approvals_purchases",
    "deposits",
    "sectors",
    "ingresses",
    "approvals_ingresses",
  ]),
  async (req, res) => {
    const { companyName, username, allowedModules } = req.session.user;
    const html = renderAppShell({
      title: "Logistica",
      subtitle: "Operacion de productos, abastecimiento y clientes",
      companyName,
      username,
      allowedModules,
      activeGroup: "logistics",
      activeSection: "",
      body: `<p class="muted">Selecciona un apartado logistico desde el menu lateral.</p>`,
    });
    res.send(renderLayout("Logistica", html));
  }
);

app.get("/dashboard", requireAuth, requireModule("dashboard"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;

  const counts = await all(
    `SELECT 
      COUNT(*) as total,
      SUM(CASE WHEN status = 'Abierto' THEN 1 ELSE 0 END) as abiertos,
      SUM(CASE WHEN status = 'En proceso' THEN 1 ELSE 0 END) as en_proceso,
      SUM(CASE WHEN status = 'Cerrado' THEN 1 ELSE 0 END) as cerrados
    FROM claims WHERE company_id = ?;`,
    [companyId]
  );

  const metrics = counts[0] || { total: 0, abiertos: 0, en_proceso: 0, cerrados: 0 };
  const claims = await all(
    "SELECT code, customer_name, product, status, created_at FROM claims WHERE company_id = ? ORDER BY id DESC LIMIT 10;",
    [companyId]
  );

  const claimRows = claims
    .map(
      (c) => `<tr>
      <td>${escapeHtml(c.code)}</td>
      <td>${escapeHtml(c.customer_name)}</td>
      <td>${escapeHtml(c.product)}</td>
      <td>${escapeHtml(c.status)}</td>
      <td>${escapeHtml(c.created_at)}</td>
    </tr>`
    )
    .join("");

  const lowStockProducts = await all(
    `SELECT name, current_stock, reorder_level
     FROM products
     WHERE company_id = ? AND current_stock <= reorder_level
     ORDER BY current_stock ASC, name ASC
     LIMIT 8;`,
    [companyId]
  );
  const lowStockRows = lowStockProducts
    .map(
      (p) => `<tr><td>${escapeHtml(p.name)}</td><td>${p.current_stock}</td><td>${p.reorder_level}</td></tr>`
    )
    .join("");

  const html = renderAppShell({
    title: "Dashboard Post Venta",
    subtitle: "Resumen operativo y registro rapido de casos",
    companyName,
    username,
    allowedModules,
    activeGroup: "home",
    activeSection: "dashboard",
    body: `
      <div class="row">
        <div class="card">Total casos<strong>${metrics.total || 0}</strong></div>
        <div class="card">Abiertos<strong>${metrics.abiertos || 0}</strong></div>
        <div class="card">En proceso<strong>${metrics.en_proceso || 0}</strong></div>
        <div class="card">Cerrados<strong>${metrics.cerrados || 0}</strong></div>
      </div>
      <h3>Registrar nuevo caso</h3>
      <form method="post" action="/claims">
        <div class="form-grid">
          <input name="customerName" placeholder="Cliente" required />
          <input name="product" placeholder="Electrodomestico" required />
          <input name="issue" placeholder="Problema reportado" required />
          <select name="status" required>
            <option>Abierto</option>
            <option>En proceso</option>
            <option>Cerrado</option>
          </select>
        </div>
        <button type="submit">Guardar caso</button>
      </form>
      <h3 style="margin-top:18px">Ultimos casos de tu empresa</h3>
      <table>
        <thead><tr><th>Codigo</th><th>Cliente</th><th>Producto</th><th>Estado</th><th>Fecha</th></tr></thead>
        <tbody>${claimRows || "<tr><td colspan='5'>Sin registros</td></tr>"}</tbody>
      </table>
      <h3 style="margin-top:18px">Alertas de bajo stock</h3>
      <table>
        <thead><tr><th>Producto</th><th>Stock actual</th><th>Minimo</th></tr></thead>
        <tbody>${lowStockRows || "<tr><td colspan='3'>No hay alertas por ahora</td></tr>"}</tbody>
      </table>`,
  });
  res.send(renderLayout("Dashboard", html));
});

app.post("/claims", requireAuth, async (req, res) => {
  const { customerName, product, issue, status } = req.body;
  const companyId = req.session.user.companyId;
  const row = await get("SELECT IFNULL(MAX(id), 0) + 1 AS next_id FROM claims WHERE company_id = ?;", [companyId]);
  const code = `PV-${String(row.next_id).padStart(4, "0")}`;

  await run(
    "INSERT INTO claims(code, customer_name, product, issue, status, company_id) VALUES (?, ?, ?, ?, ?, ?);",
    [code, customerName, product, issue, status, companyId]
  );
  res.redirect("/dashboard");
});

app.get("/workers", requireAuth, requireModule("workers"), async (req, res) => {
  const { companyId, companyName, allowedModules } = req.session.user;
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const workers = await all(
    `SELECT id, first_name, last_name, document_type, document_number, phone, email, status
     FROM workers WHERE company_id = ? ORDER BY id DESC;`,
    [companyId]
  );

  const rows = workers
    .map(
      (w) => `<tr>
      <td>${w.id}</td>
      <td>${escapeHtml(`${w.last_name}, ${w.first_name}`)}</td>
      <td>${escapeHtml(w.document_type)} ${escapeHtml(w.document_number)}</td>
      <td>${escapeHtml(w.phone || "-")}</td>
      <td>${escapeHtml(w.email || "-")}</td>
      <td>${escapeHtml(w.status)}</td>
    </tr>`
    )
    .join("");

  const html = renderAppShell({
    title: "Talento Humano",
    subtitle: "Registro y consulta de trabajadores",
    companyName,
    username: req.session.user.username,
    allowedModules,
    activeGroup: "human-talent",
    activeSection: "workers",
    body: `
      ${ok}
      ${error}
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <h3>Registrar trabajador</h3>
        <a href="/exports/workers" class="badge">Exportar Excel</a>
      </div>
      <form method="post" action="/workers">
        <div class="form-grid">
          <input name="firstName" placeholder="Nombres" required />
          <input name="lastName" placeholder="Apellidos" required />
          <select name="documentType" required>
            <option value="DNI">DNI</option>
            <option value="CE">CE</option>
            <option value="PAS">PAS</option>
          </select>
          <input name="documentNumber" placeholder="Numero documento" required />
          <input name="phone" placeholder="Celular / Telefono" />
          <input name="email" placeholder="Correo" />
        </div>
        <button type="submit">Guardar trabajador</button>
      </form>
      <h3 style="margin-top:18px">Trabajadores activos</h3>
      <table>
        <thead><tr><th>ID</th><th>Nombre</th><th>Documento</th><th>Telefono</th><th>Correo</th><th>Estado</th></tr></thead>
        <tbody>${rows || "<tr><td colspan='6'>Sin trabajadores registrados</td></tr>"}</tbody>
      </table>`,
  });
  res.send(renderLayout("Trabajadores", html));
});

app.post("/workers", requireAuth, requireModule("workers"), async (req, res) => {
  const { firstName, lastName, documentType, documentNumber, phone, email } = req.body;
  const companyId = req.session.user.companyId;
  try {
    await run(
      `INSERT INTO workers(company_id, first_name, last_name, document_type, document_number, phone, email)
       VALUES (?, ?, ?, ?, ?, ?, ?);`,
      [companyId, firstName, lastName, documentType, documentNumber, phone || "", email || ""]
    );
    return res.redirect("/workers?ok=Trabajador+registrado+correctamente");
  } catch {
    return res.redirect("/workers?error=Documento+ya+registrado+en+esta+empresa");
  }
});

app.get("/users", requireAuth, requireModule("users"), async (req, res) => {
  const { companyId, companyName, allowedModules } = req.session.user;
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";

  const workers = await all(
    `SELECT w.id, w.first_name, w.last_name, w.document_number
     FROM workers w
     LEFT JOIN users u ON u.worker_id = w.id
     WHERE w.company_id = ? AND u.id IS NULL
     ORDER BY w.last_name, w.first_name;`,
    [companyId]
  );
  const workerOptions = workers
    .map(
      (w) =>
        `<option value="${w.id}">${escapeHtml(
          `${w.last_name}, ${w.first_name} (${w.document_number})`
        )}</option>`
    )
    .join("");

  const profiles = await all(
    "SELECT id, name FROM profiles WHERE company_id = ? ORDER BY name;",
    [companyId]
  );
  const profileOptions = profiles
    .map((p) => `<option value="${p.id}">${escapeHtml(p.name)}</option>`)
    .join("");

  const users = await all(
    `SELECT u.id, u.username, u.status, p.name AS profile_name, w.first_name, w.last_name
     FROM users u
     LEFT JOIN profiles p ON p.id = u.profile_id
     LEFT JOIN workers w ON w.id = u.worker_id
     WHERE u.company_id = ?
     ORDER BY u.id DESC;`,
    [companyId]
  );
  const rows = users
    .map(
      (u) => `<tr>
      <td>${u.id}</td>
      <td>${escapeHtml(u.username)}</td>
      <td>${escapeHtml(u.profile_name || u.role || "Sin perfil")}</td>
      <td>${escapeHtml(u.last_name && u.first_name ? `${u.last_name}, ${u.first_name}` : "Sin trabajador")}</td>
      <td>${escapeHtml(u.status || "ACTIVO")}</td>
      <td><a class="badge" href="/users/${u.id}/edit">Editar</a></td>
      <td>
        <form method="post" action="/users/${u.id}/toggle-status" style="margin:0;">
          <button type="submit" class="btn-compact">${u.status === "ACTIVO" ? "Desactivar" : "Activar"}</button>
        </form>
      </td>
    </tr>`
    )
    .join("");

  const html = renderAppShell({
    title: "Gestion de Usuarios",
    subtitle: "Alta de credenciales y perfil de acceso",
    companyName,
    username: req.session.user.username,
    allowedModules,
    activeGroup: "user-management",
    activeSection: "users",
    body: `
      ${ok}
      ${error}
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <h3>Crear usuario desde trabajador</h3>
        <a href="/exports/users" class="badge">Exportar Excel</a>
      </div>
      <form method="post" action="/users">
        <div class="form-grid">
          <select name="workerId" required>
            <option value="">Selecciona trabajador</option>
            ${workerOptions}
          </select>
          <input name="username" placeholder="Username unico global" required />
          <input name="password" placeholder="Contrasena temporal" required />
          <select name="profileId" required>
            <option value="">Selecciona perfil</option>
            ${profileOptions}
          </select>
        </div>
        <button type="submit">Crear usuario</button>
      </form>
      <h3 style="margin-top:18px">Usuarios de la empresa</h3>
      <table>
        <thead><tr><th>ID</th><th>Username</th><th>Perfil</th><th>Trabajador</th><th>Estado</th><th>Editar</th><th>Activar/Desactivar</th></tr></thead>
        <tbody>${rows || "<tr><td colspan='7'>Sin usuarios creados</td></tr>"}</tbody>
      </table>`,
  });
  res.send(renderLayout("Usuarios", html));
});

app.post("/users", requireAuth, requireModule("users"), async (req, res) => {
  const { workerId, username, password, profileId } = req.body;
  const companyId = req.session.user.companyId;
  const profile = await get("SELECT id FROM profiles WHERE id = ? AND company_id = ?;", [
    Number(profileId),
    companyId,
  ]);
  if (!profile) {
    return res.redirect("/users?error=Perfil+invalido+para+tu+empresa");
  }

  const selectedWorker = await get(
    "SELECT id FROM workers WHERE id = ? AND company_id = ?;",
    [Number(workerId), companyId]
  );
  if (!selectedWorker) {
    return res.redirect("/users?error=Trabajador+no+valido+para+tu+empresa");
  }

  const existsUsername = await get("SELECT id FROM users WHERE username = ?;", [username]);
  if (existsUsername) {
    return res.redirect("/users?error=El+username+ya+existe+en+el+sistema");
  }

  const existsWorkerUser = await get("SELECT id FROM users WHERE worker_id = ?;", [Number(workerId)]);
  if (existsWorkerUser) {
    return res.redirect("/users?error=El+trabajador+ya+tiene+usuario");
  }

  await run(
    "INSERT INTO users(username, password, company_id, worker_id, role, profile_id) VALUES (?, ?, ?, ?, ?, ?);",
    [username, password, companyId, Number(workerId), "agent", Number(profileId)]
  );
  return res.redirect("/users?ok=Usuario+creado+correctamente");
});

app.get("/users/:id/edit", requireAuth, requireModule("users"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const userId = Number(req.params.id);
  const user = await get(
    `SELECT u.id, u.username, u.profile_id, w.first_name, w.last_name
     FROM users u
     LEFT JOIN workers w ON w.id = u.worker_id
     WHERE u.id = ? AND u.company_id = ?;`,
    [userId, companyId]
  );
  if (!user) return res.redirect("/users?error=Usuario+no+encontrado");

  const profiles = await all("SELECT id, name FROM profiles WHERE company_id = ? ORDER BY name;", [companyId]);
  const profileOptions = profiles
    .map((p) => `<option value="${p.id}" ${p.id === user.profile_id ? "selected" : ""}>${escapeHtml(p.name)}</option>`)
    .join("");

  const html = renderAppShell({
    title: "Editar usuario",
    subtitle: `Usuario ${user.username}`,
    companyName,
    username,
    allowedModules,
    activeGroup: "user-management",
    activeSection: "users",
    body: `
      <form method="post" action="/users/${user.id}/edit">
        <div class="form-grid">
          <input value="${escapeHtml(user.username)}" disabled />
          <input value="${escapeHtml(user.last_name && user.first_name ? `${user.last_name}, ${user.first_name}` : "Sin trabajador")}" disabled />
          <select name="profileId" required>
            ${profileOptions}
          </select>
          <input type="password" name="password" placeholder="Nueva contrasena (opcional)" />
        </div>
        <button type="submit">Guardar cambios</button>
      </form>`,
  });
  res.send(renderLayout("Editar usuario", html));
});

app.post("/users/:id/edit", requireAuth, requireModule("users"), async (req, res) => {
  const { companyId } = req.session.user;
  const userId = Number(req.params.id);
  const { profileId, password } = req.body;

  const user = await get("SELECT id FROM users WHERE id = ? AND company_id = ?;", [userId, companyId]);
  if (!user) return res.redirect("/users?error=Usuario+no+encontrado");

  const profile = await get("SELECT id FROM profiles WHERE id = ? AND company_id = ?;", [Number(profileId), companyId]);
  if (!profile) return res.redirect("/users?error=Perfil+invalido");

  if (password && String(password).trim().length > 0) {
    await run("UPDATE users SET profile_id = ?, password = ? WHERE id = ? AND company_id = ?;", [
      Number(profileId),
      String(password).trim(),
      userId,
      companyId,
    ]);
  } else {
    await run("UPDATE users SET profile_id = ? WHERE id = ? AND company_id = ?;", [Number(profileId), userId, companyId]);
  }

  return res.redirect("/users?ok=Usuario+actualizado+correctamente");
});

app.post("/users/:id/toggle-status", requireAuth, requireModule("users"), async (req, res) => {
  const { companyId, id: currentUserId } = req.session.user;
  const userId = Number(req.params.id);
  if (currentUserId === userId) return res.redirect("/users?error=No+puedes+desactivarte+a+ti+mismo");
  const user = await get("SELECT status FROM users WHERE id = ? AND company_id = ?;", [userId, companyId]);
  if (!user) return res.redirect("/users?error=Usuario+no+encontrado");
  const next = user.status === "ACTIVO" ? "INACTIVO" : "ACTIVO";
  await run("UPDATE users SET status = ? WHERE id = ? AND company_id = ?;", [next, userId, companyId]);
  return res.redirect(`/users?ok=Usuario+${next === "ACTIVO" ? "activado" : "desactivado"}`);
});

app.get("/brands", requireAuth, requireModule("brands"), async (req, res) => {
  const { companyId, companyName, allowedModules, username } = req.session.user;
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const brands = await all("SELECT id, name, status FROM brands WHERE company_id = ? ORDER BY id DESC;", [companyId]);
  const rows = brands
    .map(
      (b) => `<tr><td>${b.id}</td><td>${escapeHtml(b.name)}</td><td><span class="badge">${escapeHtml(
        b.status
      )}</span></td></tr>`
    )
    .join("");

  const html = renderAppShell({
    title: "Marcas",
    subtitle: "Catalogo de marcas de electrodomesticos",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "brands",
    body: `
      ${ok}${error}
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <h3>Registrar marca</h3>
        <a href="/exports/brands" class="badge">Exportar Excel</a>
      </div>
      <form method="post" action="/brands">
        <div class="form-grid">
          <input name="name" placeholder="Nombre de marca" required />
          <select name="status"><option>ACTIVO</option><option>INACTIVO</option></select>
        </div>
        <button type="submit">Guardar marca</button>
      </form>
      <h3 style="margin-top:18px">Marcas registradas</h3>
      <table><thead><tr><th>ID</th><th>Marca</th><th>Estado</th></tr></thead><tbody>${
        rows || "<tr><td colspan='3'>Sin marcas</td></tr>"
      }</tbody></table>`,
  });
  res.send(renderLayout("Marcas", html));
});

app.post("/brands", requireAuth, requireModule("brands"), async (req, res) => {
  const { companyId } = req.session.user;
  const { name, status } = req.body;
  try {
    await run("INSERT INTO brands(company_id, name, status) VALUES (?, ?, ?);", [
      companyId,
      (name || "").trim(),
      status || "ACTIVO",
    ]);
    return res.redirect("/brands?ok=Marca+registrada");
  } catch {
    return res.redirect("/brands?error=No+se+pudo+registrar+marca+(duplicada)");
  }
});

app.get("/categories", requireAuth, requireModule("categories"), async (req, res) => {
  const { companyId, companyName, allowedModules, username } = req.session.user;
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const categories = await all(
    "SELECT id, name, status, created_at FROM categories WHERE company_id = ? ORDER BY id DESC;",
    [companyId]
  );
  const rows = categories
    .map(
      (c) => `<tr><td>${c.id}</td><td>${escapeHtml(c.name)}</td><td>${escapeHtml(c.status)}</td><td>${escapeHtml(
        c.created_at
      )}</td></tr>`
    )
    .join("");

  const html = renderAppShell({
    title: "Categorias",
    subtitle: "Clasificacion de productos",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "categories",
    body: `
      ${ok}${error}
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <h3>Registrar categoria</h3>
        <a href="/exports/categories" class="badge">Exportar Excel</a>
      </div>
      <form method="post" action="/categories">
        <div class="form-grid">
          <input name="name" placeholder="Nombre categoria" required />
          <select name="status"><option>ACTIVO</option><option>INACTIVO</option></select>
        </div>
        <button type="submit">Guardar categoria</button>
      </form>
      <h3 style="margin-top:18px">Categorias registradas</h3>
      <table><thead><tr><th>ID</th><th>Categoria</th><th>Estado</th><th>Fecha</th></tr></thead><tbody>${
        rows || "<tr><td colspan='4'>Sin categorias</td></tr>"
      }</tbody></table>`,
  });
  res.send(renderLayout("Categorias", html));
});

app.post("/categories", requireAuth, requireModule("categories"), async (req, res) => {
  const { companyId } = req.session.user;
  const { name, status } = req.body;
  try {
    await run("INSERT INTO categories(company_id, name, status) VALUES (?, ?, ?);", [
      companyId,
      (name || "").trim(),
      status || "ACTIVO",
    ]);
    return res.redirect("/categories?ok=Categoria+registrada");
  } catch {
    return res.redirect("/categories?error=No+se+pudo+registrar+categoria+(duplicada)");
  }
});

app.get("/suppliers", requireAuth, requireModule("suppliers"), async (req, res) => {
  const { companyId, companyName, allowedModules, username } = req.session.user;
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const suppliers = await all(
    "SELECT id, name, ruc, contact_name, phone, email, status FROM suppliers WHERE company_id = ? ORDER BY id DESC;",
    [companyId]
  );
  const rows = suppliers
    .map(
      (s) => `<tr><td>${s.id}</td><td>${escapeHtml(s.name)}</td><td>${escapeHtml(
        s.ruc || "-"
      )}</td><td>${escapeHtml(s.contact_name || "-")}</td><td>${escapeHtml(s.phone || "-")}</td><td>${escapeHtml(
        s.email || "-"
      )}</td><td>${escapeHtml(s.status)}</td></tr>`
    )
    .join("");

  const html = renderAppShell({
    title: "Proveedores",
    subtitle: "Base de proveedores por empresa",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "suppliers",
    body: `
      ${ok}${error}
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <h3>Registrar proveedor</h3>
        <a href="/exports/suppliers" class="badge">Exportar Excel</a>
      </div>
      <form method="post" action="/suppliers">
        <div class="form-grid">
          <input name="name" placeholder="Razon social" required />
          <input name="ruc" placeholder="RUC" maxlength="20" />
          <input name="contactName" placeholder="Contacto" />
          <input name="phone" placeholder="Telefono" />
          <input name="email" placeholder="Correo" />
        </div>
        <button type="submit">Guardar proveedor</button>
      </form>
      <h3 style="margin-top:18px">Proveedores</h3>
      <table><thead><tr><th>ID</th><th>Proveedor</th><th>RUC</th><th>Contacto</th><th>Telefono</th><th>Correo</th><th>Estado</th></tr></thead><tbody>${
        rows || "<tr><td colspan='7'>Sin proveedores</td></tr>"
      }</tbody></table>`,
  });
  res.send(renderLayout("Proveedores", html));
});

app.post("/suppliers", requireAuth, requireModule("suppliers"), async (req, res) => {
  const { companyId } = req.session.user;
  const { name, ruc, contactName, phone, email } = req.body;
  try {
    await run(
      "INSERT INTO suppliers(company_id, name, ruc, contact_name, phone, email, status) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVO');",
      [companyId, (name || "").trim(), String(ruc || "").trim() || null, contactName || "", phone || "", email || ""]
    );
    return res.redirect("/suppliers?ok=Proveedor+registrado");
  } catch {
    return res.redirect("/suppliers?error=No+se+pudo+registrar+proveedor+(duplicado)");
  }
});

// Customers module removed (not used)

app.get("/deposits", requireAuth, requireModule("deposits"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const rowsData = await all("SELECT id, name, status, created_at FROM deposits WHERE company_id=? ORDER BY id DESC;", [companyId]);
  const rows = rowsData
    .map((d) => `<tr><td>${d.id}</td><td>${escapeHtml(d.name)}</td><td>${escapeHtml(d.status)}</td><td>${escapeHtml(d.created_at)}</td></tr>`)
    .join("");
  const html = renderAppShell({
    title: "Depositos",
    subtitle: "Almacenes fisicos para ingresos",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "deposits",
    body: `${ok}${error}
      <div style="display:flex;justify-content:space-between;align-items:center;"><h3>Registrar deposito</h3><a href="/exports/deposits" class="badge">Exportar Excel</a></div>
      <form method="post" action="/deposits"><div class="form-grid"><input name="name" placeholder="Nombre deposito" required /><select name="status"><option>ACTIVO</option><option>INACTIVO</option></select></div><button type="submit">Guardar deposito</button></form>
      <table><thead><tr><th>ID</th><th>Deposito</th><th>Estado</th><th>Fecha</th></tr></thead><tbody>${rows || "<tr><td colspan='4'>Sin depositos</td></tr>"}</tbody></table>`,
  });
  res.send(renderLayout("Depositos", html));
});

app.post("/deposits", requireAuth, requireModule("deposits"), async (req, res) => {
  const { companyId } = req.session.user;
  try {
    await run("INSERT INTO deposits(company_id, name, status) VALUES (?, ?, ?);", [companyId, req.body.name, req.body.status || "ACTIVO"]);
    return res.redirect("/deposits?ok=Deposito+registrado");
  } catch {
    return res.redirect("/deposits?error=No+se+pudo+registrar+deposito");
  }
});

app.get("/sectors", requireAuth, requireModule("sectors"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const deposits = await all("SELECT id, name FROM deposits WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const options = deposits.map((d) => `<option value="${d.id}">${escapeHtml(d.name)}</option>`).join("");
  const rowsData = await all(
    `SELECT s.id, s.name, s.status, d.name AS deposit_name, s.created_at
     FROM sectors s JOIN deposits d ON d.id=s.deposit_id
     WHERE s.company_id=? ORDER BY s.id DESC;`,
    [companyId]
  );
  const rows = rowsData
    .map((s) => `<tr><td>${s.id}</td><td>${escapeHtml(s.name)}</td><td>${escapeHtml(s.deposit_name)}</td><td>${escapeHtml(s.status)}</td><td>${escapeHtml(s.created_at)}</td></tr>`)
    .join("");
  const html = renderAppShell({
    title: "Sectores",
    subtitle: "Zonas internas por deposito",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "sectors",
    body: `${ok}${error}
      <div style="display:flex;justify-content:space-between;align-items:center;"><h3>Registrar sector</h3><a href="/exports/sectors" class="badge">Exportar Excel</a></div>
      <form method="post" action="/sectors"><div class="form-grid"><select name="depositId" required><option value="">Deposito</option>${options}</select><input name="name" placeholder="Nombre sector" required /><select name="status"><option>ACTIVO</option><option>INACTIVO</option></select></div><button type="submit">Guardar sector</button></form>
      <table><thead><tr><th>ID</th><th>Sector</th><th>Deposito</th><th>Estado</th><th>Fecha</th></tr></thead><tbody>${rows || "<tr><td colspan='5'>Sin sectores</td></tr>"}</tbody></table>`,
  });
  res.send(renderLayout("Sectores", html));
});

app.post("/sectors", requireAuth, requireModule("sectors"), async (req, res) => {
  const { companyId } = req.session.user;
  try {
    await run("INSERT INTO sectors(company_id, deposit_id, name, status) VALUES (?, ?, ?, ?);", [
      companyId,
      Number(req.body.depositId),
      req.body.name,
      req.body.status || "ACTIVO",
    ]);
    return res.redirect("/sectors?ok=Sector+registrado");
  } catch {
    return res.redirect("/sectors?error=No+se+pudo+registrar+sector");
  }
});

app.get("/products", requireAuth, requireModule("products"), async (req, res) => {
  const { companyId, companyName, allowedModules, username } = req.session.user;
  const q = (req.query.q || "").toString().trim();
  const filterCategoryId = Number(req.query.categoryId || 0);
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const brands = await all("SELECT id, name FROM brands WHERE company_id = ? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const categories = await all(
    "SELECT id, name FROM categories WHERE company_id = ? AND status='ACTIVO' ORDER BY name;",
    [companyId]
  );
  const whereFilters = ["p.company_id = ?"];
  const params = [companyId];
  if (q) {
    whereFilters.push("(p.name LIKE ? OR p.model LIKE ?)");
    params.push(`%${q}%`, `%${q}%`);
  }
  if (filterCategoryId) {
    whereFilters.push("p.category_id = ?");
    params.push(filterCategoryId);
  }
  const products = await all(
    `SELECT p.id, p.sku, p.barcode, p.image_path, p.name, p.category, p.model, p.current_stock, p.reorder_level, p.requires_serial, p.status, b.name AS brand_name, c.name AS category_name
     FROM products p
     LEFT JOIN brands b ON b.id = p.brand_id
     LEFT JOIN categories c ON c.id = p.category_id
     WHERE ${whereFilters.join(" AND ")}
     ORDER BY p.id DESC;`,
    params
  );
  const brandOptions = brands.map((b) => `<option value="${b.id}">${escapeHtml(b.name)}</option>`).join("");
  const categoryOptions = categories
    .map((c) => `<option value="${c.id}" ${filterCategoryId === c.id ? "selected" : ""}>${escapeHtml(c.name)}</option>`)
    .join("");
  const rows = products
    .map(
      (p) => `<tr><td>${p.id}</td><td>${
        p.image_path ? `<img src="${escapeHtml(p.image_path)}" style="width:38px;height:38px;object-fit:cover;border-radius:6px;" />` : "-"
      }</td><td>${escapeHtml(p.name)}</td><td>${escapeHtml(
        p.brand_name || "-"
      )}</td><td>${escapeHtml(p.category_name || p.category || "-")}</td><td>${escapeHtml(
        p.model || "-"
      )}</td><td>${p.current_stock}</td><td>${p.reorder_level}</td><td>${p.requires_serial ? "SI" : "NO"}</td><td>${escapeHtml(
        p.status === "INACTIVO" ? "Inactivo" : "Activo"
      )}</td><td>
        <a class="badge" href="/products/${p.id}/edit">Editar</a>
      </td></tr>`
    )
    .join("");

  const html = renderAppShell({
    title: "Productos",
    subtitle: "Catalogo central de productos para post venta",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "products",
    body: `
      ${ok}${error}
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <h3>Productos</h3>
        <div class="action-row">
          <a href="/exports/products" class="badge">Exportar Excel</a>
          <button type="button" class="btn-compact" onclick="openProductModal()">+ Nuevo producto</button>
        </div>
      </div>
      <div id="productModalBackdrop" style="display:none;position:fixed;inset:0;background:rgba(15,23,42,.55);z-index:80;"></div>
      <div id="productModal" style="display:none;position:fixed;inset:0;z-index:81;align-items:flex-start;justify-content:center;padding:28px;">
        <div style="width:min(900px,96vw);background:#fff;border-radius:14px;box-shadow:0 24px 80px rgba(15,23,42,.35);overflow:hidden;">
          <div style="display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid #e5e7eb;">
            <div>
              <div style="font-weight:800;font-size:16px;">Nuevo producto</div>
              <div class="muted" style="margin-top:2px;">Completa los datos del producto</div>
            </div>
            <button type="button" class="btn-compact" onclick="closeProductModal()">Cerrar</button>
          </div>
          <div style="padding:16px;">
            <form method="post" action="/products" enctype="multipart/form-data">
              <div class="form-grid">
                <input name="name" placeholder="Nombre producto" required />
                <select name="categoryId"><option value="">Categoria</option>${categories
                  .map((c) => `<option value="${c.id}">${escapeHtml(c.name)}</option>`)
                  .join("")}</select>
                <input name="model" placeholder="Modelo" />
                <select name="brandId"><option value="">Marca</option>${brandOptions}</select>
                <input type="number" name="reorderLevel" placeholder="Stock minimo alerta" min="0" value="5" />
                <label style="display:flex;align-items:center;gap:8px;"><input type="checkbox" name="requiresSerial" value="1" style="width:auto;" /> Requiere serie en ingreso</label>
                <div>
                  <input type="file" name="photo" id="productPhoto" accept="image/*" />
                  <img id="productPhotoPreview" style="display:none;margin-top:8px;width:90px;height:90px;object-fit:cover;border-radius:8px;" />
                </div>
              </div>
              <div class="action-row" style="margin-top:10px;justify-content:flex-end;">
                <button type="submit">Guardar producto</button>
              </div>
            </form>
          </div>
        </div>
      </div>

      <h3 style="margin-top:14px">Filtros</h3>
      <form method="get" action="/products">
        <div class="form-grid">
          <input name="q" value="${escapeHtml(q)}" placeholder="Buscar por nombre o modelo" />
          <select name="categoryId"><option value="">Todas las categorias</option>${categoryOptions}</select>
        </div>
        <button class="btn-compact" type="submit">Filtrar productos</button>
      </form>
      <script>
        function openProductModal() {
          const b = document.getElementById('productModalBackdrop');
          const m = document.getElementById('productModal');
          if (b) b.style.display = 'block';
          if (m) m.style.display = 'flex';
        }
        function closeProductModal() {
          const b = document.getElementById('productModalBackdrop');
          const m = document.getElementById('productModal');
          if (b) b.style.display = 'none';
          if (m) m.style.display = 'none';
        }
        document.getElementById('productModalBackdrop')?.addEventListener('click', closeProductModal);
        (function(){
          const input = document.getElementById('productPhoto');
          const img = document.getElementById('productPhotoPreview');
          if (input && img) {
            input.addEventListener('change', function(){
              const file = this.files && this.files[0];
              if (!file) { img.style.display='none'; img.src=''; return; }
              img.src = URL.createObjectURL(file);
              img.style.display='block';
            });
          }
        })();
      </script>
      <table style="margin-top:12px"><thead><tr><th>ID</th><th>Foto</th><th>Producto</th><th>Marca</th><th>Categoria</th><th>Modelo</th><th>Stock</th><th>Minimo</th><th>Serie</th><th>Estado</th><th>Accion</th></tr></thead><tbody>${
        rows || "<tr><td colspan='11'>Sin productos</td></tr>"
      }</tbody></table>`,
  });
  res.send(renderLayout("Productos", html));
});

app.post("/products", requireAuth, requireModule("products"), upload.single("photo"), async (req, res) => {
  const { companyId } = req.session.user;
  const { name, categoryId, model, brandId, reorderLevel, requiresSerial } = req.body;
  try {
    const seq = await get("SELECT IFNULL(MAX(id),0)+1 AS next_id FROM products WHERE company_id = ?;", [companyId]);
    const sku = `SKU-${String(seq.next_id).padStart(5, "0")}`;
    const autoBarcode = `BAR-${companyId}-${String(seq.next_id).padStart(8, "0")}`;
    const imagePath = req.file ? `/uploads/products/${req.file.filename}` : "";
    await run(
      `INSERT INTO products(company_id, brand_id, category_id, supplier_id, sku, barcode, name, category, model, current_stock, reorder_level, requires_serial, image_path, status)
       VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, 0, ?, ?, ?, 'ACTIVO');`,
      [
        companyId,
        brandId ? Number(brandId) : null,
        categoryId ? Number(categoryId) : null,
        sku,
        autoBarcode,
        (name || "").trim(),
        "",
        model || "",
        Number(reorderLevel || 5),
        requiresSerial ? 1 : 0,
        imagePath,
      ]
    );
    return res.redirect("/products?ok=Producto+registrado");
  } catch {
    return res.redirect("/products?error=No+se+pudo+registrar+producto+(SKU+duplicado)");
  }
});

app.get("/products/:id/edit", requireAuth, requireModule("products"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const productId = Number(req.params.id);
  const product = await get("SELECT * FROM products WHERE id=? AND company_id=?;", [productId, companyId]);
  if (!product) return res.redirect("/products?error=Producto+no+encontrado");
  const brands = await all("SELECT id, name FROM brands WHERE company_id = ? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const categories = await all("SELECT id, name FROM categories WHERE company_id = ? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const brandOptions = brands
    .map((b) => `<option value="${b.id}" ${product.brand_id === b.id ? "selected" : ""}>${escapeHtml(b.name)}</option>`)
    .join("");
  const categoryOptions = categories
    .map((c) => `<option value="${c.id}" ${product.category_id === c.id ? "selected" : ""}>${escapeHtml(c.name)}</option>`)
    .join("");
  const html = renderAppShell({
    title: "Editar producto",
    subtitle: `${product.name}`,
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "products",
    body: `<form method="post" action="/products/${productId}/edit" enctype="multipart/form-data"><div class="form-grid">
      <input name="name" value="${escapeHtml(product.name)}" required />
      <select name="categoryId">${categoryOptions}</select>
      <input name="model" value="${escapeHtml(product.model || "")}" />
      <select name="brandId">${brandOptions}</select>
      <input type="number" name="reorderLevel" min="0" value="${product.reorder_level}" />
      <label style="display:flex;flex-direction:column;gap:4px;">
        <span class="muted">Estado en catalogo</span>
        <select name="productStatus">
          <option value="ACTIVO" ${product.status !== "INACTIVO" ? "selected" : ""}>Activo (visible en solicitudes)</option>
          <option value="INACTIVO" ${product.status === "INACTIVO" ? "selected" : ""}>Inactivo (oculto en solicitudes)</option>
        </select>
      </label>
      <label style="display:flex;align-items:center;gap:8px;"><input type="checkbox" name="requiresSerial" value="1" style="width:auto;" ${
        product.requires_serial ? "checked" : ""
      } /> Requiere serie en ingreso</label>
      <div>
        ${product.image_path ? `<img id="productPhotoPreviewEdit" src="${escapeHtml(
          product.image_path
        )}" style="width:90px;height:90px;object-fit:cover;border-radius:8px;" />` : `<img id="productPhotoPreviewEdit" style="display:none;width:90px;height:90px;object-fit:cover;border-radius:8px;" /><span id="productPhotoNoImage" class="muted">Sin foto cargada</span>`}
        <input type="file" name="photo" id="productPhotoEdit" accept="image/*" style="margin-top:8px" />
      </div>
    </div><button type="submit">Guardar cambios</button></form>
    <script>
      (function(){
        const inputEdit = document.getElementById('productPhotoEdit');
        const imgEdit = document.getElementById('productPhotoPreviewEdit');
        const noImg = document.getElementById('productPhotoNoImage');
        if (inputEdit && imgEdit) {
          inputEdit.addEventListener('change', function(){
            const file = this.files && this.files[0];
            if (!file) return;
            imgEdit.src = URL.createObjectURL(file);
            imgEdit.style.display='block';
            if (noImg) noImg.style.display='none';
          });
        }
      })();
    </script>`,
  });
  res.send(renderLayout("Editar producto", html));
});

app.post("/products/:id/edit", requireAuth, requireModule("products"), upload.single("photo"), async (req, res) => {
  const { companyId } = req.session.user;
  const productId = Number(req.params.id);
  const { name, categoryId, model, brandId, reorderLevel, requiresSerial, productStatus } = req.body;
  const catalogStatus = productStatus === "INACTIVO" ? "INACTIVO" : "ACTIVO";
  const current = await get("SELECT sku, barcode FROM products WHERE id=? AND company_id=?;", [productId, companyId]);
  if (!current) return res.redirect("/products?error=Producto+no+encontrado");
  const imagePath = req.file ? `/uploads/products/${req.file.filename}` : null;
  try {
    if (imagePath) {
      await run(
        `UPDATE products
         SET sku=?, barcode=?, name=?, category_id=?, model=?, brand_id=?, reorder_level=?, requires_serial=?, image_path=?, status=?
         WHERE id=? AND company_id=?;`,
        [
          current.sku,
          current.barcode || null,
          (name || "").trim(),
          categoryId ? Number(categoryId) : null,
          model || "",
          brandId ? Number(brandId) : null,
          Number(reorderLevel || 5),
          requiresSerial ? 1 : 0,
          imagePath,
          catalogStatus,
          productId,
          companyId,
        ]
      );
    } else {
      await run(
        `UPDATE products
         SET sku=?, barcode=?, name=?, category_id=?, model=?, brand_id=?, reorder_level=?, requires_serial=?, status=?
         WHERE id=? AND company_id=?;`,
        [
          current.sku,
          current.barcode || null,
          (name || "").trim(),
          categoryId ? Number(categoryId) : null,
          model || "",
          brandId ? Number(brandId) : null,
          Number(reorderLevel || 5),
          requiresSerial ? 1 : 0,
          catalogStatus,
          productId,
          companyId,
        ]
      );
    }
    return res.redirect("/products?ok=Producto+actualizado");
  } catch {
    return res.redirect("/products?error=No+se+pudo+actualizar+producto");
  }
});

app.get("/products/:id/view", requireAuth, requireModule("products"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const productId = Number(req.params.id);
  const product = await get(
    `SELECT p.*, b.name AS brand_name, c.name AS category_name
     FROM products p
     LEFT JOIN brands b ON b.id=p.brand_id
     LEFT JOIN categories c ON c.id=p.category_id
     WHERE p.id=? AND p.company_id=?;`,
    [productId, companyId]
  );
  if (!product) return res.redirect("/products?error=Producto+no+encontrado");
  const byLocation = await all(
    `SELECT d.name AS deposit_name, s.name AS sector_name, il.quantity
     FROM inventory_locations il
     JOIN deposits d ON d.id=il.deposit_id
     JOIN sectors s ON s.id=il.sector_id
     WHERE il.company_id=? AND il.product_id=?
     ORDER BY d.name, s.name;`,
    [companyId, productId]
  );
  const serials = await all(
    `SELECT serial_number, status, created_at
     FROM product_serials
     WHERE company_id=? AND product_id=?
     ORDER BY id DESC LIMIT 100;`,
    [companyId, productId]
  );
  const locationRows = byLocation
    .map((r) => `<tr><td>${escapeHtml(r.deposit_name)}</td><td>${escapeHtml(r.sector_name)}</td><td>${r.quantity}</td></tr>`)
    .join("");
  const serialRows = serials
    .map((s) => `<tr><td>${escapeHtml(s.serial_number)}</td><td>${escapeHtml(s.status)}</td><td>${escapeHtml(s.created_at)}</td></tr>`)
    .join("");
  const html = renderAppShell({
    title: "Detalle de producto",
    subtitle: `${product.name}`,
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "products",
    body: `
      <div class="form-grid">
        <input value="${escapeHtml(product.name)}" disabled />
        <input value="${escapeHtml(product.brand_name || "-")}" disabled />
        <input value="${escapeHtml(product.category_name || "-")}" disabled />
        <input value="${escapeHtml(product.model || "-")}" disabled />
        <input value="${product.current_stock}" disabled />
      </div>
      ${product.image_path ? `<img src="${escapeHtml(product.image_path)}" style="width:130px;height:130px;object-fit:cover;border-radius:10px;" />` : "<p class='muted'>Sin foto</p>"}
      <div class="action-row">
        <a class="badge" href="/products/${product.id}/edit">Editar</a>
      </div>
      <h3>Stock por deposito/sector</h3>
      <table><thead><tr><th>Deposito</th><th>Sector</th><th>Cantidad</th></tr></thead><tbody>${locationRows || "<tr><td colspan='3'>Sin ubicaciones</td></tr>"}</tbody></table>
      <h3 style="margin-top:14px">Series</h3>
      <table><thead><tr><th>Serie</th><th>Estado</th><th>Fecha ingreso</th></tr></thead><tbody>${serialRows || "<tr><td colspan='3'>Sin series</td></tr>"}</tbody></table>`,
  });
  res.send(renderLayout("Detalle producto", html));
});

app.get("/products/:id/code-pdf", requireAuth, requireModule("products"), async (req, res) => {
  return res.status(410).send("La generacion de codigos fue retirada.");
});

app.get("/products/codes-pdf", requireAuth, requireModule("products"), async (req, res) => {
  return res.status(410).send("La generacion de codigos fue retirada.");
});

app.get("/scan/product", async (req, res) => {
  return res.status(410).send("La consulta por codigo fue retirada.");
});

app.get("/stock", requireAuth, requireModule("stock"), async (req, res) => {
  const { companyId, companyName, allowedModules, username } = req.session.user;
  const from = req.query.from || "";
  const to = req.query.to || "";
  const productId = Number(req.query.productId || 0);
  const depositId = Number(req.query.depositId || 0);
  const sectorId = Number(req.query.sectorId || 0);
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const products = await all("SELECT id, name, current_stock FROM products WHERE company_id = ? ORDER BY name;", [companyId]);
  const deposits = await all("SELECT id, name FROM deposits WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const sectors = await all("SELECT id, name, deposit_id FROM sectors WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const productOptions = products
    .map((p) => `<option value="${p.id}" ${productId === p.id ? "selected" : ""}>${escapeHtml(`${p.name} (stock: ${p.current_stock})`)}</option>`)
    .join("");
  const depositOptions = deposits
    .map((d) => `<option value="${d.id}" ${depositId === d.id ? "selected" : ""}>${escapeHtml(d.name)}</option>`)
    .join("");
  const sectorOptions = sectors
    .map((s) => `<option value="${s.id}" data-deposit="${s.deposit_id}" ${sectorId === s.id ? "selected" : ""}>${escapeHtml(s.name)}</option>`)
    .join("");

  const locationFilters = ["il.company_id=?"];
  const locationParams = [companyId];
  if (productId) { locationFilters.push("il.product_id=?"); locationParams.push(productId); }
  if (depositId) { locationFilters.push("il.deposit_id=?"); locationParams.push(depositId); }
  if (sectorId) { locationFilters.push("il.sector_id=?"); locationParams.push(sectorId); }
  const stockByLocation = await all(
    `SELECT p.name, d.name AS deposit_name, s.name AS sector_name, il.quantity
     FROM inventory_locations il
     JOIN products p ON p.id=il.product_id
     JOIN deposits d ON d.id=il.deposit_id
     JOIN sectors s ON s.id=il.sector_id
     WHERE ${locationFilters.join(" AND ")}
     ORDER BY p.name, d.name, s.name;`,
    locationParams
  );
  const locationRows = stockByLocation
    .map(
      (r) => `<tr><td>${escapeHtml(r.name)}</td><td>${escapeHtml(r.deposit_name)}</td><td>${escapeHtml(
        r.sector_name
      )}</td><td>${r.quantity}</td></tr>`
    )
    .join("");

  const html = renderAppShell({
    title: "Stock por Producto",
    subtitle: "Reporte de stock por ubicacion",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "stock",
    body: `
      ${ok}${error}
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <h3>Filtros</h3>
        <a href="/exports/stock?productId=${encodeURIComponent(productId || "")}&depositId=${encodeURIComponent(
          depositId || ""
        )}&sectorId=${encodeURIComponent(sectorId || "")}&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}" class="badge">Exportar Excel</a>
      </div>
      <form method="get" action="/stock">
        <div class="form-grid">
          <select name="productId"><option value="">Producto</option>${productOptions}</select>
          <select name="depositId" id="sDeposit"><option value="">Deposito</option>${depositOptions}</select>
          <select name="sectorId" id="sSector"><option value="">Sector</option>${sectorOptions}</select>
          <input type="date" name="from" value="${escapeHtml(from)}" />
          <input type="date" name="to" value="${escapeHtml(to)}" />
        </div>
        <button type="submit">Aplicar filtro</button>
      </form>
      <script>
        (function(){
          const dep = document.getElementById('sDeposit');
          const sec = document.getElementById('sSector');
          function apply(){
            const d = dep?.value || '';
            sec?.querySelectorAll('option[data-deposit]')?.forEach(o => { o.hidden = d && o.dataset.deposit !== d; });
          }
          dep?.addEventListener('change', apply);
          apply();
        })();
      </script>
      <h3 style="margin-top:12px">Stock por deposito y sector</h3>
      <table><thead><tr><th>Producto</th><th>Deposito</th><th>Sector</th><th>Stock</th></tr></thead><tbody>${
        locationRows || "<tr><td colspan='4'>Sin stock ubicado</td></tr>"
      }</tbody></table>
      <div class="action-row" style="margin-top:10px">
        <a class="badge" href="/kardex">Ir a Kardex detallado</a>
      </div>`,
  });
  res.send(renderLayout("Stock", html));
});

app.post("/stock", requireAuth, requireModule("stock"), async (req, res) => {
  return res.status(410).send("El registro manual de movimientos fue deshabilitado. Usa Kardex / Compras / Ingresos.");
});

app.get("/price-history", requireAuth, requireModule("prices"), async (req, res) => {
  return res.redirect("/products?error=El+apartado+de+historial+de+precios+fue+retirado");
});

app.post("/price-history", requireAuth, requireModule("prices"), async (req, res) => {
  return res.redirect("/products?error=El+apartado+de+historial+de+precios+fue+retirado");
});

app.get("/approvers", requireAuth, requireModule("approvers"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const users = await all(
    `SELECT u.id, u.username, IFNULL(p.name, u.role) AS profile_name,
            CASE WHEN a.id IS NULL THEN 0 ELSE 1 END AS is_approver
     FROM users u
     LEFT JOIN profiles p ON p.id = u.profile_id
     LEFT JOIN approvers a ON a.user_id = u.id AND a.company_id = u.company_id AND a.status='ACTIVO'
     WHERE u.company_id = ?
     ORDER BY u.username;`,
    [companyId]
  );
  const rows = users
    .map(
      (u) => `<tr>
      <td>${u.id}</td><td>${escapeHtml(u.username)}</td><td>${escapeHtml(u.profile_name || "-")}</td>
      <td>${u.is_approver ? "SI" : "NO"}</td>
      <td>
        <form method="post" action="/approvers/toggle" style="margin:0;">
          <input type="hidden" name="userId" value="${u.id}" />
          <button type="submit">${u.is_approver ? "Quitar" : "Asignar"}</button>
        </form>
      </td>
    </tr>`
    )
    .join("");

  const html = renderAppShell({
    title: "Aprobadores",
    subtitle: "Configura quienes aprueban comprobantes",
    companyName,
    username,
    allowedModules,
    activeGroup: "user-management",
    activeSection: "approvers",
    body: `${ok}
      <a href="/exports/approvers" class="badge">Exportar Excel</a>
      <table style="margin-top:12px;"><thead><tr><th>ID</th><th>Usuario</th><th>Perfil</th><th>Aprobador</th><th>Accion</th></tr></thead><tbody>${
        rows || "<tr><td colspan='5'>Sin usuarios</td></tr>"
      }</tbody></table>`,
  });
  res.send(renderLayout("Aprobadores", html));
});

app.post("/approvers/toggle", requireAuth, requireModule("approvers"), async (req, res) => {
  const { companyId } = req.session.user;
  const userId = Number(req.body.userId);
  const exists = await get(
    "SELECT id FROM approvers WHERE company_id = ? AND user_id = ? AND status='ACTIVO';",
    [companyId, userId]
  );
  if (exists) {
    await run("UPDATE approvers SET status='INACTIVO' WHERE id = ?;", [exists.id]);
    return res.redirect("/approvers?ok=Aprobador+desactivado");
  }
  await run("INSERT OR REPLACE INTO approvers(company_id, user_id, status) VALUES (?, ?, 'ACTIVO');", [
    companyId,
    userId,
  ]);
  return res.redirect("/approvers?ok=Aprobador+asignado");
});

app.get("/requests", requireAuth, requireModule("requests"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const categories = await all("SELECT id, name FROM categories WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const products = await all("SELECT id, name, category_id FROM products WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [
    companyId,
  ]);
  const categoryOptions = categories.map((c) => `<option value="${c.id}">${escapeHtml(c.name)}</option>`).join("");
  const productOptions = products
    .map((p) => `<option value="${p.id}" data-category="${p.category_id || ""}">${escapeHtml(p.name)}</option>`)
    .join("");
  const html = renderAppShell({
    title: "Solicitudes",
    subtitle: "Solicitudes con multiples productos del catalogo",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "requests",
    body: `${ok}${error}
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <h3>Nueva solicitud</h3><a href="/exports/requests" class="badge">Exportar Excel</a>
      </div>
      <form method="post" action="/requests">
        <div class="form-grid">
          <input name="note" placeholder="Observacion general" />
        </div>
        <div style="display:grid;grid-template-columns:160px 1fr 140px 40px;gap:8px;margin:10px 0 6px 0;font-weight:700;">
          <div>Categoria</div><div>Producto</div><div>Cantidad</div><div style="text-align:center">+</div>
        </div>
        <div id="request-items">
          <div class="request-row" style="display:grid;grid-template-columns:160px 1fr 140px 40px;gap:8px;">
            <select class="reqCat" required><option value="">Categoria</option>${categoryOptions}</select>
            <select name="productId" required><option value="">Producto</option>${productOptions}</select>
            <input type="number" min="1" name="quantity" placeholder="Cantidad" required />
            <button type="button" class="btn-compact" onclick="addRequestItem()">+</button>
          </div>
        </div>
        <div class="action-row">
          <button type="submit" class="btn-compact">Registrar solicitud</button>
          <a class="badge" href="/reports?type=SOLICITUD">Ver reporte solicitudes</a>
        </div>
      </form>
      <script>
        function applyCategoryFilterRow(row) {
          const category = row.querySelector('.reqCat')?.value || '';
          const sel = row.querySelector('select[name="productId"]');
          if (!sel) return;
          sel.value = '';
          sel.querySelectorAll('option[data-category]').forEach((opt) => {
            opt.hidden = Boolean(category) && opt.dataset.category !== category;
          });
        }
        document.querySelectorAll('.request-row').forEach((row) => {
          row.querySelector('.reqCat')?.addEventListener('change', () => applyCategoryFilterRow(row));
        });
        function addRequestItem() {
          const container = document.getElementById('request-items');
          const html = \`<div class="request-row" style="display:grid;grid-template-columns:160px 1fr 140px 40px;gap:8px;margin-top:6px;"><select class="reqCat" required><option value="">Categoria</option>${categoryOptions}</select><select name="productId" required><option value="">Producto</option>${productOptions}</select><input type="number" min="1" name="quantity" placeholder="Cantidad" required /><button type="button" class="btn-compact" onclick="this.closest('.request-row').remove()">-</button></div>\`;
          container.insertAdjacentHTML('beforeend', html);
          const newRow = container.lastElementChild;
          newRow.querySelector('.reqCat')?.addEventListener('change', () => applyCategoryFilterRow(newRow));
        }
      </script>`,
  });
  res.send(renderLayout("Solicitudes", html));
});

app.post("/requests", requireAuth, requireModule("requests"), async (req, res) => {
  const { companyId, id: userId, workerId } = req.session.user;
  const productIds = toArray(req.body.productId).map((v) => Number(v)).filter((v) => Number.isInteger(v) && v > 0);
  const quantities = toArray(req.body.quantity).map((v) => Number(v));
  if (productIds.length === 0 || productIds.length !== quantities.length) {
    return res.redirect("/requests?error=Debes+registrar+productos+validos");
  }
  const code = await nextPurchaseRequestCode(companyId);
  await run(
    `INSERT INTO purchase_requests(company_id, request_code, requested_by_user_id, requested_by_worker_id, requested_item, quantity, note)
     VALUES (?, ?, ?, ?, ?, ?, ?);`,
    [companyId, code, userId, workerId || null, "MULTI", quantities.reduce((a, b) => a + b, 0), req.body.note || ""]
  );
  const request = await get("SELECT id FROM purchase_requests WHERE company_id=? AND request_code=?;", [companyId, code]);
  for (let i = 0; i < productIds.length; i += 1) {
    const product = await get("SELECT id FROM products WHERE id=? AND company_id=? AND status='ACTIVO';", [
      productIds[i],
      companyId,
    ]);
    if (!product) return res.redirect("/requests?error=Producto+invalido+o+inactivo+en+solicitud");
    await run("INSERT INTO request_items(request_id, product_id, quantity, note) VALUES (?, ?, ?, ?);", [
      request.id,
      productIds[i],
      quantities[i],
      "",
    ]);
  }
  return res.redirect("/requests?ok=Solicitud+registrada");
});

app.get("/requests/:id/edit", requireAuth, requireModule("requests"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const requestId = Number(req.params.id);
  const request = await get("SELECT * FROM purchase_requests WHERE id=? AND company_id=?;", [requestId, companyId]);
  if (!request) return res.redirect("/requests?error=Solicitud+no+encontrada");
  const hasPurchase = await get("SELECT id FROM purchases WHERE company_id=? AND request_id=? LIMIT 1;", [companyId, requestId]);
  if (hasPurchase || request.status === "ATENDIDA" || request.status === "RECHAZADA")
    return res.redirect("/requests?error=Solicitud+bloqueada+por+flujo+o+rechazo");
  if (!(request.status === "PENDIENTE" || request.status === "APROBADA"))
    return res.redirect("/requests?error=Solo+solicitudes+pendientes+o+aprobadas+sin+compra+pueden+editarse");
  const categories = await all("SELECT id, name FROM categories WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const items = await all(
    `SELECT ri.id, ri.product_id, ri.quantity, ri.note, p.name
     FROM request_items ri JOIN products p ON p.id=ri.product_id
     WHERE ri.request_id=? ORDER BY ri.id;`,
    [requestId]
  );
  const products = await all("SELECT id, name, category_id FROM products WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [
    companyId,
  ]);
  const productOptions = products
    .map((p) => `<option value="${p.id}" data-category="${p.category_id || ""}">${escapeHtml(p.name)}</option>`)
    .join("");
  const rows = items
    .map(
      (i) =>
        `<div class="form-grid"><select name="productId" required><option value="${i.product_id}">${escapeHtml(
          i.name
        )}</option>${productOptions}</select><input type="number" min="1" name="quantity" value="${i.quantity}" required/></div>`
    )
    .join("");
  const html = renderAppShell({
    title: "Editar solicitud",
    subtitle: `Comprobante ${request.request_code}`,
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "requests",
    body: `<form method="post" action="/requests/${requestId}/edit">
      <div class="form-grid"><input name="note" value="${escapeHtml(
        request.note || ""
      )}" placeholder="Observacion"/></div>
      <div id="request-items">${rows}</div>
      <div class="action-row"><button type="button" class="btn-compact" onclick="addEditRequestItem()">+ Agregar producto</button><button type="submit" class="btn-compact">Guardar cambios</button></div>
      <script>
        function addEditRequestItem(){document.getElementById('request-items').insertAdjacentHTML('beforeend','<div class="form-grid"><select name="productId" required><option value="">Producto</option>${productOptions}</select><input type="number" min="1" name="quantity" placeholder="Cantidad" required/></div>');}
      </script>
    </form>`,
  });
  res.send(renderLayout("Editar solicitud", html));
});

app.post("/requests/:id/edit", requireAuth, requireModule("requests"), async (req, res) => {
  const { companyId } = req.session.user;
  const requestId = Number(req.params.id);
  const request = await get("SELECT * FROM purchase_requests WHERE id=? AND company_id=?;", [requestId, companyId]);
  if (!request) return res.redirect("/requests?error=Solicitud+no+encontrada");
  const hasPurchase = await get("SELECT id FROM purchases WHERE company_id=? AND request_id=? LIMIT 1;", [companyId, requestId]);
  if (hasPurchase || request.status === "ATENDIDA" || request.status === "RECHAZADA")
    return res.redirect("/requests?error=Solicitud+bloqueada+por+flujo+posterior");
  if (!(request.status === "PENDIENTE" || request.status === "APROBADA"))
    return res.redirect("/requests?error=No+se+puede+editar+en+este+estado");
  const productIds = toArray(req.body.productId).map((v) => Number(v)).filter((v) => Number.isInteger(v) && v > 0);
  const quantities = toArray(req.body.quantity).map((v) => Number(v));
  if (productIds.length === 0 || productIds.length !== quantities.length) return res.redirect("/requests?error=Items+invalidos");
  await run(
    "UPDATE purchase_requests SET status='PENDIENTE', approved_by_user_id=NULL, approved_at=NULL, note=?, quantity=? WHERE id=? AND company_id=?;",
    [req.body.note || "", quantities.reduce((a, b) => a + b, 0), requestId, companyId]
  );
  await run("DELETE FROM request_items WHERE request_id=?;", [requestId]);
  for (let i = 0; i < productIds.length; i += 1) {
    const product = await get("SELECT id FROM products WHERE id=? AND company_id=? AND status='ACTIVO';", [
      productIds[i],
      companyId,
    ]);
    if (!product) return res.redirect("/requests?error=Producto+invalido+o+inactivo+en+solicitud");
    await run("INSERT INTO request_items(request_id, product_id, quantity, note) VALUES (?, ?, ?, ?);", [
      requestId,
      productIds[i],
      quantities[i],
      "",
    ]);
  }
  return res.redirect("/requests?ok=Solicitud+actualizada+y+enviada+a+nueva+aprobacion");
});

app.get("/requests/:id/pdf", requireAuth, requireModule("requests"), async (req, res) => {
  const { companyId, companyName, companyRuc } = req.session.user;
  const requestId = Number(req.params.id);
  const request = await get(
    "SELECT request_code, status, note, created_at FROM purchase_requests WHERE id=? AND company_id=?;",
    [requestId, companyId]
  );
  if (!request) return res.status(404).send("Solicitud no encontrada");
  const items = await all(
    `SELECT p.name, ri.quantity, ri.note,
            IFNULL(b.name,'-') AS brand_name, IFNULL(c.name,'-') AS cat_name,
            IFNULL(TRIM(p.model),'') AS model_txt
     FROM request_items ri
     JOIN products p ON p.id=ri.product_id
     LEFT JOIN brands b ON b.id=p.brand_id AND b.company_id=p.company_id
     LEFT JOIN categories c ON c.id=p.category_id AND c.company_id=p.company_id
     WHERE ri.request_id=?;`,
    [requestId]
  );
  return sendVoucherPdf(res, `${request.request_code}.pdf`, {
    title: "Solicitud de Compra",
    code: request.request_code,
    date: request.created_at,
    status: request.status,
    companyName: companyName || "Empresa",
    companyRuc: companyRuc || "",
    companyInfo: "Post Venta — documento oficial",
    metaLines: [`Observacion: ${request.note || "-"}`],
    items: items.map((i) => ({
      name: i.name,
      quantity: i.quantity,
      unitPrice: 0,
      total: 0,
      brandName: i.brand_name,
      categoryName: i.cat_name,
      modelName: i.model_txt || "-",
    })),
    showPrices: false,
    footer: "Documento de solicitud generado por sistema.",
  });
});

app.get("/approvals/requests", requireAuth, requireModule("approvals_requests"), async (req, res) => {
  const { companyId, companyName, username, allowedModules, id: userId } = req.session.user;
  if (!(await isApprover(companyId, userId))) return res.redirect("/requests?error=No+eres+aprobador");
  const rowsData = await all(
    `SELECT r.id, r.request_code, r.note, r.created_at, u.username AS solicitante,
            IFNULL(SUM(ri.quantity),0) AS total_qty, IFNULL(COUNT(ri.id),0) AS items
     FROM purchase_requests r
     JOIN users u ON u.id = r.requested_by_user_id
     LEFT JOIN request_items ri ON ri.request_id = r.id
     WHERE r.company_id=? AND r.status='PENDIENTE'
     GROUP BY r.id
     ORDER BY r.id DESC;`,
    [companyId]
  );
  const rows = rowsData
    .map(
      (r) => `<tr><td>${escapeHtml(r.request_code)}</td><td>${r.items}</td><td>${r.total_qty}</td><td>${escapeHtml(
        r.solicitante
      )}</td><td>${escapeHtml(r.note || "-")}</td><td>${escapeHtml(r.created_at)}</td>
      <td class="action-cell">
        <div class="approval-actions">
          <a href="/requests/${r.id}/pdf" class="badge">Ver PDF</a>
          <form method="post" action="/requests/${r.id}/approve"><button type="submit" class="btn-compact">Aprobar</button></form>
          <form method="post" action="/requests/${r.id}/reject"><button type="submit" class="btn-compact danger-button">Rechazar</button></form>
        </div>
      </td></tr>`
    )
    .join("");
  const html = renderAppShell({
    title: "Aprobacion de Solicitudes",
    subtitle: "Bandeja separada de aprobacion",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "approvals_requests",
    body: `<table><thead><tr><th>Solicitud</th><th>Items</th><th>Cantidad</th><th>Solicitante</th><th>Nota</th><th>Fecha</th><th>Accion</th></tr></thead><tbody>${
      rows || "<tr><td colspan='7'>Sin solicitudes pendientes</td></tr>"
    }</tbody></table>`,
  });
  res.send(renderLayout("Aprobacion Solicitudes", html));
});

app.post("/requests/:id/approve", requireAuth, requireModule("approvals_requests"), async (req, res) => {
  const { companyId, id: userId } = req.session.user;
  if (!(await isApprover(companyId, userId))) return res.redirect("/approvals/requests?error=No+eres+aprobador");
  await run(
    "UPDATE purchase_requests SET status='APROBADA', approved_by_user_id=?, approved_at=CURRENT_TIMESTAMP WHERE id=? AND company_id=?;",
    [userId, Number(req.params.id), companyId]
  );
  return res.redirect("/approvals/requests?ok=Solicitud+aprobada");
});

app.post("/requests/:id/reject", requireAuth, requireModule("approvals_requests"), async (req, res) => {
  const { companyId, id: userId } = req.session.user;
  if (!(await isApprover(companyId, userId))) return res.redirect("/approvals/requests?error=No+eres+aprobador");
  await run(
    "UPDATE purchase_requests SET status='RECHAZADA', approved_by_user_id=?, approved_at=CURRENT_TIMESTAMP WHERE id=? AND company_id=?;",
    [userId, Number(req.params.id), companyId]
  );
  return res.redirect("/approvals/requests?ok=Solicitud+rechazada");
});

app.get("/purchases", requireAuth, requireModule("purchases"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const selectedRequestId = Number(req.query.requestId || 0);
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const suppliers = await all("SELECT id, name FROM suppliers WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const approvedRequests = await all(
    `SELECT r.id, r.request_code, r.note
     FROM purchase_requests r
     WHERE r.company_id=? AND r.status='APROBADA'
       AND NOT EXISTS (SELECT 1 FROM purchases p WHERE p.company_id=r.company_id AND p.request_id=r.id)
     ORDER BY r.id DESC;`,
    [companyId]
  );
  const selectedRequest = approvedRequests.find((r) => r.id === selectedRequestId) || null;
  const requestItems = selectedRequestId
    ? await all(
        `SELECT ri.product_id, ri.quantity, p.name
         FROM request_items ri JOIN products p ON p.id=ri.product_id
         WHERE ri.request_id=?
         ORDER BY ri.id;`,
        [selectedRequestId]
      )
    : [];
  const supplierOptions = suppliers.map((s) => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join("");
  const requestOptions = approvedRequests
    .map((r) => `<option value="${r.id}" ${r.id === selectedRequestId ? "selected" : ""}>${escapeHtml(r.request_code)}</option>`)
    .join("");

  const html = renderAppShell({
    title: "Compras",
    subtitle: "Comprobante de compra con multiples productos",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "purchases",
    body: `${ok}${error}
      <div style="display:flex;justify-content:space-between;align-items:center;"><h3>Nueva compra</h3><a href="/exports/purchases" class="badge">Exportar Excel</a></div>
      <form method="get" action="/purchases">
        <div class="form-grid">
          <select name="requestId" required><option value="">Selecciona solicitud aprobada</option>${requestOptions}</select>
        </div>
        <button type="submit" class="btn-compact">Cargar solicitud</button>
      </form>
      <form method="post" action="/purchases">
        <input type="hidden" name="requestId" value="${selectedRequestId || ""}" />
        <div class="form-grid">
          <select name="supplierId" required><option value="">Proveedor (obligatorio)</option>${supplierOptions}</select>
          <input value="${selectedRequestId ? `Solicitud ${selectedRequestId}` : "Selecciona solicitud aprobada"}" disabled />
          <input value="${escapeHtml(selectedRequest?.note || "Sin observacion de solicitud")}" disabled />
        </div>
        <div id="purchase-items">
          ${
            requestItems.length > 0
              ? requestItems
                  .map(
                    (it) => `<div class="form-grid">
                      <input value="${escapeHtml(it.name)}" disabled />
                      <input value="${it.quantity}" disabled />
                      <input type="hidden" name="productId" value="${it.product_id}" />
                      <input type="hidden" name="quantity" value="${it.quantity}" />
                      <input type="number" step="0.01" name="unitPrice" min="0" placeholder="Precio unitario" required/>
                    </div>`
                  )
                  .join("")
              : "<p class='muted'>Primero selecciona una solicitud aprobada para cargar sus items.</p>"
          }
        </div>
        <div class="action-row">
          <button type="submit" class="btn-compact" ${requestItems.length === 0 ? "disabled" : ""}>Registrar compra</button>
          <a class="badge" href="/reports?type=COMPRA">Ver reporte compras</a>
        </div>
      </form>
      `,
  });
  res.send(renderLayout("Compras", html));
});

app.post("/purchases", requireAuth, requireModule("purchases"), async (req, res) => {
  const { companyId, id: userId } = req.session.user;
  const requestId = Number(req.body.requestId || 0);
  if (!requestId) return res.redirect("/purchases?error=La+compra+debe+venir+de+una+solicitud+aprobada");
  const sourceRequest = await get(
    "SELECT id, status, note FROM purchase_requests WHERE id=? AND company_id=?;",
    [requestId, companyId]
  );
  if (!sourceRequest || sourceRequest.status !== "APROBADA")
    return res.redirect("/purchases?error=Solicitud+no+valida+para+compra");
  const existsPurchase = await get("SELECT id FROM purchases WHERE company_id=? AND request_id=?;", [companyId, requestId]);
  if (existsPurchase) return res.redirect("/purchases?error=La+solicitud+ya+tiene+compra+registrada");
  const productIds = toArray(req.body.productId).map((v) => Number(v)).filter((v) => Number.isInteger(v) && v > 0);
  const quantities = toArray(req.body.quantity).map((v) => Number(v));
  const prices = toArray(req.body.unitPrice).map((v) => Number(v));
  if (productIds.length === 0 || productIds.length !== quantities.length || quantities.length !== prices.length) {
    return res.redirect("/purchases?error=Items+de+compra+invalidos");
  }
  const supplierId = Number(req.body.supplierId || 0);
  if (!Number.isInteger(supplierId) || supplierId < 1) {
    return res.redirect("/purchases?error=Debes+seleccionar+proveedor");
  }
  const supplierOk = await get("SELECT id FROM suppliers WHERE id=? AND company_id=? AND status='ACTIVO';", [
    supplierId,
    companyId,
  ]);
  if (!supplierOk) return res.redirect("/purchases?error=Proveedor+invalido");
  const voucherCode = await nextPurchaseVoucherCode(companyId);
  const total = quantities.reduce((acc, q, idx) => acc + q * prices[idx], 0);
  await run(
    `INSERT INTO purchases(company_id, voucher_code, supplier_id, request_id, product_id, quantity, unit_price, total, created_by_user_id, status)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDIENTE_APROBACION');`,
    [
      companyId,
      voucherCode,
      supplierId,
      requestId,
      productIds[0],
      quantities[0],
      prices[0],
      total,
      userId,
    ]
  );
  const purchase = await get("SELECT id FROM purchases WHERE company_id=? AND voucher_code=?;", [companyId, voucherCode]);
  for (let i = 0; i < productIds.length; i += 1) {
    await run(
      "INSERT INTO purchase_items(purchase_id, product_id, quantity, unit_price, total, note) VALUES (?, ?, ?, ?, ?, ?);",
      [purchase.id, productIds[i], quantities[i], prices[i], quantities[i] * prices[i], sourceRequest.note || ""]
    );
  }
  return res.redirect("/purchases?ok=Compra+registrada+y+pendiente+de+aprobacion");
});

app.get("/purchases/:id/edit", requireAuth, requireModule("purchases"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const purchaseId = Number(req.params.id);
  const purchase = await get("SELECT * FROM purchases WHERE id=? AND company_id=?;", [purchaseId, companyId]);
  if (!purchase) return res.redirect("/purchases?error=Compra+no+encontrada");
  if (purchase.status === "INGRESADA" || purchase.status === "RECHAZADA" || purchase.status === "PENDIENTE_APROB_INGRESO")
    return res.redirect("/purchases?error=Compra+bloqueada+por+estado");
  const suppliers = await all("SELECT id,name FROM suppliers WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const products = await all("SELECT id,sku,name FROM products WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const items = await all("SELECT * FROM purchase_items WHERE purchase_id=? ORDER BY id;", [purchaseId]);
  const supplierOptions = suppliers.map((s) => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join("");
  const productOptions = products.map((p) => `<option value="${p.id}">${escapeHtml(p.name)}</option>`).join("");
  const itemRows = items
    .map(
      (i) =>
        `<div class="form-grid"><select name="productId" required><option value="${i.product_id}">Actual</option>${productOptions}</select><input type="number" name="quantity" min="1" value="${i.quantity}" required/><input type="number" step="0.01" name="unitPrice" min="0" value="${i.unit_price}" required/></div>`
    )
    .join("");
  const html = renderAppShell({
    title: "Editar compra",
    subtitle: `Comprobante ${purchase.voucher_code}`,
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "purchases",
    body: `<form method="post" action="/purchases/${purchaseId}/edit"><div class="form-grid"><select name="supplierId" required><option value="${purchase.supplier_id || ""}">Proveedor actual</option>${supplierOptions}</select></div><div id="purchase-items">${itemRows}</div><button type="submit">Guardar cambios (requiere reaprobacion)</button></form>`,
  });
  res.send(renderLayout("Editar compra", html));
});

app.post("/purchases/:id/edit", requireAuth, requireModule("purchases"), async (req, res) => {
  const { companyId } = req.session.user;
  const purchaseId = Number(req.params.id);
  const purchase = await get("SELECT * FROM purchases WHERE id=? AND company_id=?;", [purchaseId, companyId]);
  if (!purchase) return res.redirect("/purchases?error=Compra+no+encontrada");
  if (purchase.status === "INGRESADA" || purchase.status === "RECHAZADA" || purchase.status === "PENDIENTE_APROB_INGRESO")
    return res.redirect("/purchases?error=Compra+no+editable+en+este+estado");
  const productIds = toArray(req.body.productId).map((v) => Number(v)).filter((v) => Number.isInteger(v) && v > 0);
  const quantities = toArray(req.body.quantity).map((v) => Number(v));
  const prices = toArray(req.body.unitPrice).map((v) => Number(v));
  if (productIds.length === 0 || productIds.length !== quantities.length || quantities.length !== prices.length)
    return res.redirect("/purchases?error=Items+invalidos");
  const supplierId = Number(req.body.supplierId || 0);
  if (!Number.isInteger(supplierId) || supplierId < 1)
    return res.redirect("/purchases?error=Debes+seleccionar+proveedor");
  const supplierOk = await get("SELECT id FROM suppliers WHERE id=? AND company_id=? AND status='ACTIVO';", [
    supplierId,
    companyId,
  ]);
  if (!supplierOk) return res.redirect("/purchases?error=Proveedor+invalido");
  const total = quantities.reduce((acc, q, idx) => acc + q * prices[idx], 0);
  await run(
    `UPDATE purchases
     SET supplier_id=?, product_id=?, quantity=?, unit_price=?, total=?, status='PENDIENTE_APROBACION',
         approved_by_user_id=NULL, approved_at=NULL, receipt_code=NULL, stock_received_at=NULL, received_by_user_id=NULL,
         ingress_doc_type=NULL, ingress_doc_series=NULL, ingress_doc_pdf=NULL,
         ingress_rejection_note=NULL, ingress_rejected_at=NULL, ingress_rejected_by_user_id=NULL
     WHERE id=? AND company_id=?;`,
    [
      supplierId,
      productIds[0],
      quantities[0],
      prices[0],
      total,
      purchaseId,
      companyId,
    ]
  );
  await run("DELETE FROM product_serials WHERE company_id=? AND purchase_id=? AND status='PENDIENTE_INGRESO';", [companyId, purchaseId]);
  await run("DELETE FROM purchase_items WHERE purchase_id=?;", [purchaseId]);
  for (let i = 0; i < productIds.length; i += 1) {
    await run("INSERT INTO purchase_items(purchase_id, product_id, quantity, unit_price, total, note) VALUES (?, ?, ?, ?, ?, ?);", [
      purchaseId,
      productIds[i],
      quantities[i],
      prices[i],
      quantities[i] * prices[i],
      "",
    ]);
  }
  return res.redirect("/purchases?ok=Compra+editada+y+enviada+a+reaprobacion");
});

app.get("/approvals/purchases", requireAuth, requireModule("approvals_purchases"), async (req, res) => {
  const { companyId, companyName, username, allowedModules, id: userId } = req.session.user;
  if (!(await isApprover(companyId, userId))) return res.redirect("/purchases?error=No+eres+aprobador");
  const rowsData = await all(
    `SELECT p.id, p.voucher_code, p.total, p.created_at, IFNULL(s.name,'-') AS supplier_name, cu.username AS creador
     FROM purchases p
     LEFT JOIN suppliers s ON s.id=p.supplier_id
     JOIN users cu ON cu.id=p.created_by_user_id
     WHERE p.company_id=? AND p.status='PENDIENTE_APROBACION'
     ORDER BY p.id DESC;`,
    [companyId]
  );
  const rows = rowsData
    .map(
      (p) => `<tr><td>${escapeHtml(p.voucher_code)}</td><td>${p.total}</td><td>${escapeHtml(p.supplier_name)}</td><td>${escapeHtml(
        p.creador
      )}</td><td>${escapeHtml(p.created_at)}</td>
      <td class="action-cell">
        <div class="approval-actions">
          <a href="/purchases/${p.id}/pdf" class="badge">Ver PDF</a>
          <form method="post" action="/purchases/${p.id}/approve"><button type="submit" class="btn-compact">Aprobar compra</button></form>
          <form method="post" action="/purchases/${p.id}/reject"><button type="submit" class="btn-compact danger-button">Rechazar compra</button></form>
        </div>
      </td></tr>`
    )
    .join("");
  const html = renderAppShell({
    title: "Aprobacion de Compras",
    subtitle: "Bandeja separada de aprobacion de comprobantes",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "approvals_purchases",
    body: `<table><thead><tr><th>Comprobante</th><th>Total</th><th>Proveedor</th><th>Creador</th><th>Fecha</th><th>Accion</th></tr></thead><tbody>${
      rows || "<tr><td colspan='6'>Sin compras pendientes</td></tr>"
    }</tbody></table>`,
  });
  res.send(renderLayout("Aprobacion Compras", html));
});

app.get("/approvals/ingresses", requireAuth, requireModule("approvals_ingresses"), async (req, res) => {
  const { companyId, companyName, username, allowedModules, id: userId } = req.session.user;
  if (!(await isApprover(companyId, userId))) return res.redirect("/purchases?error=No+eres+aprobador");
  const rowsData = await all(
    `SELECT p.id, p.voucher_code, p.total, p.created_at, IFNULL(s.name,'-') AS supplier_name, cu.username AS creador,
            IFNULL(p.ingress_doc_type,'') AS ingress_doc_type, IFNULL(p.ingress_doc_series,'') AS ingress_doc_series,
            IFNULL(p.ingress_doc_pdf,'') AS ingress_doc_pdf
     FROM purchases p
     LEFT JOIN suppliers s ON s.id=p.supplier_id
     JOIN users cu ON cu.id=p.created_by_user_id
     WHERE p.company_id=? AND p.status='PENDIENTE_APROB_INGRESO'
     ORDER BY p.id DESC;`,
    [companyId]
  );
  const rows = rowsData
    .map((p) => {
      const pdfIng =
        p.ingress_doc_pdf && String(p.ingress_doc_pdf).trim()
          ? `<a href="${escapeHtml(p.ingress_doc_pdf)}" class="badge" target="_blank" rel="noopener">PDF SUNAT</a>`
          : "";
      const docHint =
        p.ingress_doc_type && p.ingress_doc_series
          ? `<span class="muted">${escapeHtml(p.ingress_doc_type)} ${escapeHtml(p.ingress_doc_series)}</span>`
          : "";
      return `<tr><td>${escapeHtml(p.voucher_code)}</td><td>${p.total}</td><td>${escapeHtml(p.supplier_name)}</td><td>${escapeHtml(
        p.creador
      )}</td><td>${escapeHtml(p.created_at)}</td><td>${docHint}</td>
      <td class="action-cell">
        <div class="approval-actions">
          <a href="/purchases/${p.id}/pdf" class="badge">OC compra</a>
          ${pdfIng}
          <form method="post" action="/purchases/${p.id}/approve-ingress"><button type="submit" class="btn-compact">Aprobar ingreso</button></form>
          <details class="ingress-reject-details">
            <summary>Rechazar ingreso</summary>
            <form method="post" action="/purchases/${p.id}/reject-ingress" class="reject-inline">
              <label class="muted">Observacion del rechazo (obligatoria)</label>
              <textarea name="observation" required maxlength="2000" placeholder="Ej. comprobante ilegible, no coincide con la OC..."></textarea>
              <button type="submit" class="btn-compact danger-button">Confirmar rechazo</button>
            </form>
          </details>
        </div>
      </td></tr>`;
    })
    .join("");
  const html = renderAppShell({
    title: "Aprobacion de Ingresos",
    subtitle: "Bandeja de aprobacion previa al ingreso fisico",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "approvals_ingresses",
    body: `<table><thead><tr><th>Comprobante</th><th>Total</th><th>Proveedor</th><th>Creador</th><th>Fecha</th><th>Doc. ingreso</th><th>Acciones</th></tr></thead><tbody>${
      rows || "<tr><td colspan='7'>Sin ingresos pendientes</td></tr>"
    }</tbody></table>`,
  });
  res.send(renderLayout("Aprobacion Ingresos", html));
});

app.get("/ingresses", requireAuth, requireModule("ingresses"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const rowsData = await all(
    `SELECT p.id, p.voucher_code, p.total, p.status, IFNULL(p.receipt_code,'-') AS receipt_code, p.created_at
     FROM purchases p
     WHERE p.company_id=? AND p.status IN ('APROBADA','INGRESO_RECHAZADO')
     ORDER BY p.id DESC;`,
    [companyId]
  );
  const rows = rowsData
    .map((p) => {
      const st =
        p.status === "INGRESO_RECHAZADO"
          ? `<span class="badge" style="background:#fee2e2;color:#991b1b">Ingreso rechazado</span>`
          : escapeHtml(p.status);
      return `<tr><td>${escapeHtml(p.voucher_code)}</td><td>${st}</td><td>${p.total}</td><td>${escapeHtml(
        p.created_at
      )}</td><td><a class="badge" href="/purchases/${p.id}/receive">${
        p.status === "INGRESO_RECHAZADO" ? "Reintentar ingreso" : "Ingresar stock"
      }</a></td></tr>`;
    })
    .join("");
  const html = renderAppShell({
    title: "Ingresos",
    subtitle: "Recepcion de compras aprobadas para ingreso",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "ingresses",
    body: `${ok}${error}<div class="action-row"><a class="badge" href="/reports?type=INGRESO">Ver reporte ingresos</a></div>
    <table><thead><tr><th>Comprobante</th><th>Estado</th><th>Total</th><th>Fecha</th><th>Accion</th></tr></thead><tbody>${
      rows || "<tr><td colspan='5'>Sin ingresos por procesar</td></tr>"
    }</tbody></table>`,
  });
  res.send(renderLayout("Ingresos", html));
});

app.post("/purchases/:id/approve", requireAuth, requireModule("approvals_purchases"), async (req, res) => {
  const { companyId, id: userId } = req.session.user;
  if (!(await isApprover(companyId, userId))) return res.redirect("/approvals/purchases?error=No+eres+aprobador");
  await run(
    "UPDATE purchases SET status='APROBADA', approved_by_user_id=?, approved_at=CURRENT_TIMESTAMP WHERE id=? AND company_id=?;",
    [userId, Number(req.params.id), companyId]
  );
  return res.redirect("/approvals/purchases?ok=Compra+aprobada,+lista+para+registro+de+ingreso");
});

app.post("/purchases/:id/reject", requireAuth, requireModule("approvals_purchases"), async (req, res) => {
  const { companyId, id: userId } = req.session.user;
  if (!(await isApprover(companyId, userId))) return res.redirect("/approvals/purchases?error=No+eres+aprobador");
  await run(
    "UPDATE purchases SET status='RECHAZADA', approved_by_user_id=?, approved_at=CURRENT_TIMESTAMP WHERE id=? AND company_id=?;",
    [userId, Number(req.params.id), companyId]
  );
  return res.redirect("/approvals/purchases?ok=Compra+rechazada");
});

app.post("/purchases/:id/approve-ingress", requireAuth, requireModule("approvals_ingresses"), async (req, res) => {
  const { companyId, id: userId } = req.session.user;
  if (!(await isApprover(companyId, userId))) return res.redirect("/approvals/ingresses?error=No+eres+aprobador");
  const purchaseId = Number(req.params.id);
  const purchase = await get(
    "SELECT id, request_id, deposit_id, sector_id, ingress_doc_pdf, ingress_doc_type, ingress_doc_series FROM purchases WHERE id=? AND company_id=? AND status='PENDIENTE_APROB_INGRESO';",
    [purchaseId, companyId]
  );
  if (!purchase) return res.redirect("/approvals/ingresses?error=Compra+no+esta+pendiente+de+aprobacion+de+ingreso");
  if (!purchase.ingress_doc_pdf || !String(purchase.ingress_doc_pdf).trim()) {
    return res.redirect("/approvals/ingresses?error=Falta+PDF+de+comprobante+en+el+ingreso");
  }
  const items = await all(
    `SELECT pi.product_id, pi.quantity, p.requires_serial
     FROM purchase_items pi JOIN products p ON p.id=pi.product_id
     WHERE pi.purchase_id=?;`,
    [purchaseId]
  );
  for (const item of items) {
    if (item.requires_serial) {
      const serialCount = await get(
        "SELECT COUNT(1) AS total FROM product_serials WHERE company_id=? AND purchase_id=? AND product_id=? AND status='PENDIENTE_INGRESO';",
        [companyId, purchaseId, item.product_id]
      );
      if (Number(serialCount?.total || 0) !== Number(item.quantity)) {
        return res.redirect("/approvals/ingresses?error=Series+pendientes+incompletas+para+aprobar+ingreso");
      }
      await run(
        "UPDATE product_serials SET status='EN_STOCK', deposit_id=?, sector_id=? WHERE company_id=? AND purchase_id=? AND product_id=? AND status='PENDIENTE_INGRESO';",
        [purchase.deposit_id, purchase.sector_id, companyId, purchaseId, item.product_id]
      );
    }
    await run("UPDATE products SET current_stock = current_stock + ? WHERE id = ? AND company_id = ?;", [
      item.quantity,
      item.product_id,
      companyId,
    ]);
    await run(
      "INSERT INTO stock_movements(company_id, product_id, movement_type, quantity, note, purchase_id, deposit_id, sector_id) VALUES (?, ?, 'ENTRADA', ?, ?, ?, ?, ?);",
      [companyId, item.product_id, item.quantity, "", purchaseId, purchase.deposit_id, purchase.sector_id]
    );
    await run(
      "INSERT OR IGNORE INTO inventory_locations(company_id, product_id, deposit_id, sector_id, quantity) VALUES (?, ?, ?, ?, 0);",
      [companyId, item.product_id, purchase.deposit_id, purchase.sector_id]
    );
    await run(
      "UPDATE inventory_locations SET quantity = quantity + ?, updated_at = CURRENT_TIMESTAMP WHERE company_id=? AND product_id=? AND deposit_id=? AND sector_id=?;",
      [item.quantity, companyId, item.product_id, purchase.deposit_id, purchase.sector_id]
    );
  }
  const receiptCode = await nextIngressReceiptCode(companyId);
  await run(
    "UPDATE purchases SET status='INGRESADA', receipt_code=?, stock_received_at=CURRENT_TIMESTAMP WHERE id=? AND company_id=?;",
    [receiptCode, purchaseId, companyId]
  );
  if (purchase.request_id) {
    await run("UPDATE purchase_requests SET status='ATENDIDA' WHERE id=? AND company_id=?;", [purchase.request_id, companyId]);
  }
  return res.redirect("/approvals/ingresses?ok=Ingreso+aprobado+y+stock+actualizado");
});

app.post("/purchases/:id/reject-ingress", requireAuth, requireModule("approvals_ingresses"), async (req, res) => {
  const { companyId, id: userId } = req.session.user;
  if (!(await isApprover(companyId, userId))) return res.redirect("/approvals/ingresses?error=No+eres+aprobador");
  const purchaseId = Number(req.params.id);
  const observation = String(req.body.observation || "").trim();
  if (!observation) return res.redirect("/approvals/ingresses?error=Debes+indicar+la+observacion+del+rechazo");
  const purchase = await get(
    "SELECT id FROM purchases WHERE id=? AND company_id=? AND status='PENDIENTE_APROB_INGRESO';",
    [purchaseId, companyId]
  );
  if (!purchase) return res.redirect("/approvals/ingresses?error=Compra+no+esta+pendiente+de+aprobacion+de+ingreso");
  await run("DELETE FROM product_serials WHERE company_id=? AND purchase_id=? AND status='PENDIENTE_INGRESO';", [
    companyId,
    purchaseId,
  ]);
  await run(
    `UPDATE purchases SET status='INGRESO_RECHAZADO',
     ingress_rejection_note=?, ingress_rejected_at=CURRENT_TIMESTAMP, ingress_rejected_by_user_id=?,
     received_by_user_id=NULL, deposit_id=NULL, sector_id=NULL
     WHERE id=? AND company_id=?;`,
    [observation, userId, purchaseId, companyId]
  );
  return res.redirect("/approvals/ingresses?ok=Ingreso+rechazado+sin+afectar+stock");
});

app.get("/purchases/:id/receive", requireAuth, requireModule("ingresses"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const purchaseId = Number(req.params.id);
  const purchase = await get("SELECT id, voucher_code, status FROM purchases WHERE id=? AND company_id=?;", [purchaseId, companyId]);
  if (!purchase) return res.redirect("/purchases?error=Compra+no+encontrada");
  if (purchase.status !== "APROBADA" && purchase.status !== "INGRESO_RECHAZADO") {
    return res.redirect("/ingresses?error=Compra+debe+estar+aprobada+o+con+ingreso+rechazado+para+volver+a+registrar");
  }
  const deposits = await all("SELECT id, name FROM deposits WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const sectors = await all("SELECT id, name, deposit_id FROM sectors WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const items = await all(
    `SELECT pi.id, pi.product_id, pi.quantity, p.name, p.requires_serial
     FROM purchase_items pi JOIN products p ON p.id=pi.product_id
     WHERE pi.purchase_id=?;`,
    [purchaseId]
  );
  const depositOptions = deposits.map((d) => `<option value="${d.id}">${escapeHtml(d.name)}</option>`).join("");
  const sectorOptions = sectors.map((s) => `<option value="${s.id}" data-deposit="${s.deposit_id}">${escapeHtml(s.name)}</option>`).join("");
  const serialInputs = items
    .filter((i) => i.requires_serial)
    .map(
      (i) => `<div><label>${escapeHtml(i.name)} (cant: ${i.quantity})</label>
        <textarea name="serials_${i.id}" placeholder="Una serie por linea"></textarea></div>`
    )
    .join("");
  const html = renderAppShell({
    title: "Ingreso de compra",
    subtitle: `Comprobante ${purchase.voucher_code}`,
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "ingresses",
    body: `<form method="post" action="/purchases/${purchaseId}/receive" enctype="multipart/form-data">
      <div class="form-grid">
        <select name="depositId" id="depositId" required><option value="">Deposito</option>${depositOptions}</select>
        <select name="sectorId" id="sectorId" required><option value="">Sector</option>${sectorOptions}</select>
        <select name="ingressDocType" required>
          <option value="">Tipo comprobante de compra</option>
          <option value="BOLETA">Boleta</option>
          <option value="FACTURA">Factura</option>
        </select>
        <input name="ingressDocSeries" placeholder="Serie y numero (ej. F001-000123)" required />
        <div>
          <label class="muted">PDF del comprobante (obligatorio)</label>
          <input type="file" name="ingressPdf" accept="application/pdf,.pdf" required />
        </div>
      </div>
      ${serialInputs ? `<h3>Series requeridas</h3>${serialInputs}` : "<p class='muted'>No se requieren series en esta compra.</p>"}
      <button type="submit">Confirmar ingreso</button>
    </form>
    <script>
      document.getElementById('depositId')?.addEventListener('change', function(){
        const d = this.value;
        const options = document.querySelectorAll('#sectorId option[data-deposit]');
        options.forEach(o => { o.hidden = d && o.dataset.deposit !== d; });
      });
    </script>`,
  });
  res.send(renderLayout("Ingreso compra", html));
});

app.post(
  "/purchases/:id/receive",
  requireAuth,
  requireModule("ingresses"),
  uploadIngressDoc.single("ingressPdf"),
  async (req, res) => {
  const { companyId, id: userId } = req.session.user;
  const depositId = Number(req.body.depositId);
  const sectorId = Number(req.body.sectorId);
  const docType = String(req.body.ingressDocType || "").trim().toUpperCase();
  const docSeries = String(req.body.ingressDocSeries || "").trim();
  if (docType !== "BOLETA" && docType !== "FACTURA") {
    return res.redirect("/ingresses?error=Tipo+de+comprobante+invalido");
  }
  if (!docSeries) return res.redirect("/ingresses?error=Indica+serie+y+numero+del+comprobante");
  if (!req.file) return res.redirect("/ingresses?error=Debes+adjuntar+el+PDF+del+comprobante");
  const pdfPath = `/uploads/ingress-docs/${req.file.filename}`;
  const deposit = await get("SELECT id FROM deposits WHERE id=? AND company_id=? AND status='ACTIVO';", [depositId, companyId]);
  const sector = await get("SELECT id, deposit_id FROM sectors WHERE id=? AND company_id=? AND status='ACTIVO';", [sectorId, companyId]);
  if (!deposit || !sector || sector.deposit_id !== depositId) return res.redirect("/purchases?error=Deposito+o+sector+invalidos");
  const purchase = await get("SELECT id, status, request_id FROM purchases WHERE id=? AND company_id=?;", [
    Number(req.params.id),
    companyId,
  ]);
  if (!purchase) return res.redirect("/ingresses?error=Compra+no+encontrada");
  if (purchase.status !== "APROBADA" && purchase.status !== "INGRESO_RECHAZADO") {
    return res.redirect("/ingresses?error=Compra+no+disponible+para+ingreso");
  }
  const items = await all(
    `SELECT pi.id, pi.product_id, pi.quantity, p.requires_serial
     FROM purchase_items pi JOIN products p ON p.id=pi.product_id
     WHERE pi.purchase_id=?;`,
    [purchase.id]
  );
  for (const item of items) {
    if (item.requires_serial) {
      const lines = String(req.body[`serials_${item.id}`] || "")
        .split(/\r?\n/)
        .map((v) => v.trim())
        .filter(Boolean);
      if (lines.length !== item.quantity) {
        return res.redirect("/ingresses?error=Series+incompletas+para+items+que+requieren+serie");
      }
      const upperSerials = lines.map((x) => x.toUpperCase());
      if (new Set(upperSerials).size !== upperSerials.length) {
        return res.redirect("/ingresses?error=" + encodeURIComponent("Hay series duplicadas en las lineas del mismo producto."));
      }
      for (const serial of lines) {
        const conflictName = await findSerialConflictSameProductGroup(companyId, item.product_id, serial);
        if (conflictName) {
          return res.redirect(
            "/ingresses?error=" +
              encodeURIComponent(
                `Serie ${serial}: ya existe para "${conflictName}" con la misma marca, modelo y categoria.`
              )
          );
        }
      }
      await run("DELETE FROM product_serials WHERE company_id=? AND purchase_id=? AND product_id=? AND status='PENDIENTE_INGRESO';", [
        companyId,
        purchase.id,
        item.product_id,
      ]);
      for (const serial of lines) {
        await run(
          "INSERT INTO product_serials(company_id, product_id, serial_number, purchase_id, deposit_id, sector_id, status) VALUES (?, ?, ?, ?, ?, ?, 'PENDIENTE_INGRESO');",
          [companyId, item.product_id, serial, purchase.id, depositId, sectorId]
        );
      }
    } else {
      await run("DELETE FROM product_serials WHERE company_id=? AND purchase_id=? AND product_id=? AND status='PENDIENTE_INGRESO';", [
        companyId,
        purchase.id,
        item.product_id,
      ]);
    }
  }
  await run(
    `UPDATE purchases SET status='PENDIENTE_APROB_INGRESO', received_by_user_id=?, deposit_id=?, sector_id=?,
     ingress_doc_type=?, ingress_doc_series=?, ingress_doc_pdf=?,
     ingress_rejection_note=NULL, ingress_rejected_at=NULL, ingress_rejected_by_user_id=NULL
     WHERE id=? AND company_id=?;`,
    [userId, depositId, sectorId, docType, docSeries, pdfPath, purchase.id, companyId]
  );
  return res.redirect("/ingresses?ok=Ingreso+registrado+y+enviado+a+aprobacion");
  }
);

app.get("/purchases/:id/pdf", requireAuth, requireModule("purchases"), async (req, res) => {
  const { companyId, companyName, companyRuc } = req.session.user;
  const purchaseId = Number(req.params.id);
  const purchase = await get(
    `SELECT p.voucher_code, p.status, p.total, p.created_at, IFNULL(p.receipt_code,'-') AS receipt_code,
            IFNULL(s.name,'-') AS supplier_name, IFNULL(NULLIF(TRIM(s.ruc),''),'-') AS supplier_ruc,
            IFNULL(d.name,'-') AS deposit_name, IFNULL(se.name,'-') AS sector_name
     FROM purchases p
     LEFT JOIN suppliers s ON s.id=p.supplier_id
     LEFT JOIN deposits d ON d.id=p.deposit_id
     LEFT JOIN sectors se ON se.id=p.sector_id
     WHERE p.id=? AND p.company_id=?;`,
    [purchaseId, companyId]
  );
  if (!purchase) return res.status(404).send("Compra no encontrada");
  const items = await all(
    `SELECT p.name, pi.quantity, pi.unit_price, pi.total,
            IFNULL(b.name,'-') AS brand_name, IFNULL(c.name,'-') AS cat_name, IFNULL(TRIM(p.model),'') AS model_txt
     FROM purchase_items pi
     JOIN products p ON p.id=pi.product_id
     LEFT JOIN brands b ON b.id=p.brand_id AND b.company_id=p.company_id
     LEFT JOIN categories c ON c.id=p.category_id AND c.company_id=p.company_id
     WHERE pi.purchase_id=?;`,
    [purchaseId]
  );
  return sendVoucherPdf(res, `${purchase.voucher_code}.pdf`, {
    title: "Orden de Compra Local",
    code: purchase.voucher_code,
    date: purchase.created_at,
    status: purchase.status,
    companyName: companyName || "Empresa",
    companyRuc: companyRuc || "",
    companyInfo: "Post Venta — documento oficial",
    metaLines: [
      `Proveedor: ${purchase.supplier_name}`,
      `RUC proveedor: ${purchase.supplier_ruc || "-"}`,
      ...(purchase.deposit_name && purchase.deposit_name !== "-" ? [`Deposito: ${purchase.deposit_name}`] : []),
      ...(purchase.sector_name && purchase.sector_name !== "-" ? [`Sector: ${purchase.sector_name}`] : []),
      ...(purchase.receipt_code && purchase.receipt_code !== "-" ? [`Correlativo ingreso: ${purchase.receipt_code}`] : []),
    ],
    items: items.map((i) => ({
      name: i.name,
      quantity: i.quantity,
      unitPrice: i.unit_price,
      total: i.total,
      brandName: i.brand_name,
      categoryName: i.cat_name,
      modelName: i.model_txt || "-",
    })),
    total: purchase.total,
    footer: "Documento de compra generado por sistema.",
  });
});

app.get("/purchases/:id/ingress-pdf", requireAuth, requireModule("purchases"), async (req, res) => {
  const { companyId, companyName, companyRuc } = req.session.user;
  const purchaseId = Number(req.params.id);
  const purchase = await get(
    `SELECT p.receipt_code, p.status, p.total, p.stock_received_at,
            IFNULL(p.ingress_doc_type,'-') AS ingress_doc_type,
            IFNULL(p.ingress_doc_series,'-') AS ingress_doc_series,
            IFNULL(p.ingress_doc_pdf,'') AS ingress_doc_pdf,
            IFNULL(s.name,'-') AS supplier_name, IFNULL(NULLIF(TRIM(s.ruc),''),'-') AS supplier_ruc,
            d.name AS deposit_name, se.name AS sector_name
     FROM purchases p
     LEFT JOIN suppliers s ON s.id=p.supplier_id
     LEFT JOIN deposits d ON d.id=p.deposit_id
     LEFT JOIN sectors se ON se.id=p.sector_id
     WHERE p.id=? AND p.company_id=?;`,
    [purchaseId, companyId]
  );
  if (!purchase || !purchase.receipt_code) return res.status(404).send("Ingreso no encontrado");
  const items = await all(
    `SELECT p.id AS product_id, p.name, p.requires_serial, pi.quantity, pi.unit_price, pi.total,
            IFNULL(b.name,'-') AS brand_name, IFNULL(c.name,'-') AS cat_name, IFNULL(TRIM(p.model),'') AS model_txt
     FROM purchase_items pi
     JOIN products p ON p.id=pi.product_id
     LEFT JOIN brands b ON b.id=p.brand_id AND b.company_id=p.company_id
     LEFT JOIN categories c ON c.id=p.category_id AND c.company_id=p.company_id
     WHERE pi.purchase_id=?;`,
    [purchaseId]
  );
  const serials = await all(
    `SELECT ps.product_id, ps.serial_number
     FROM product_serials ps
     WHERE ps.company_id=? AND ps.purchase_id=? AND ps.status IN ('EN_STOCK','PENDIENTE_INGRESO')
     ORDER BY ps.product_id, ps.serial_number;`,
    [companyId, purchaseId]
  );
  const serialMap = new Map();
  for (const s of serials) {
    const k = String(s.product_id);
    if (!serialMap.has(k)) serialMap.set(k, []);
    serialMap.get(k).push(s.serial_number);
  }
  const hasPdf = Boolean(purchase.ingress_doc_pdf && String(purchase.ingress_doc_pdf).trim());
  return sendVoucherPdf(res, `${purchase.receipt_code}.pdf`, {
    title: "Comprobante de Ingreso",
    code: purchase.receipt_code,
    date: purchase.stock_received_at,
    status: purchase.status,
    companyName: companyName || "Empresa",
    companyRuc: companyRuc || "",
    companyInfo: "Post Venta — documento oficial",
    metaLines: [
      `Proveedor: ${purchase.supplier_name || "-"}`,
      `RUC proveedor: ${purchase.supplier_ruc || "-"}`,
      `Deposito: ${purchase.deposit_name || "-"}`,
      `Sector: ${purchase.sector_name || "-"}`,
      `Tipo documento (SUNAT): ${purchase.ingress_doc_type || "-"}`,
      `Serie y numero: ${purchase.ingress_doc_series || "-"}`,
      ...(hasPdf ? [`PDF comprobante SUNAT: adjunto en sistema`] : []),
    ],
    items: items.map((i) => {
      const list = Number(i.requires_serial) === 1 ? serialMap.get(String(i.product_id)) || [] : [];
      const serialLines = list.map((sn, idx) => `${idx + 1}: ${sn}`);
      return {
        name: i.name,
        quantity: i.quantity,
        unitPrice: i.unit_price,
        total: i.total,
        brandName: i.brand_name,
        categoryName: i.cat_name,
        modelName: i.model_txt || "-",
        serialLines,
      };
    }),
    total: purchase.total,
    footer: "Documento de ingreso generado por sistema.",
  });
});

app.get("/reports", requireAuth, requireModule("reports"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const type = (req.query.type || "").toString().trim().toUpperCase();
  const number = (req.query.number || "").toString().trim();
  const filters = [];
  const params = [companyId];
  if (type) {
    filters.push("report_type = ?");
    params.push(type);
  }
  if (number) {
    filters.push("voucher_code LIKE ?");
    params.push(`%${number}%`);
  }
  const rows = await all(
    `SELECT * FROM (
      SELECT 'SOLICITUD' AS report_type, r.id AS source_id, r.request_code AS voucher_code, r.status, r.created_at,
             IFNULL(r.note,'-') AS note, '/requests/' || r.id || '/pdf' AS pdf_path,
             CASE WHEN r.status IN ('PENDIENTE','APROBADA')
                  AND NOT EXISTS (SELECT 1 FROM purchases p WHERE p.company_id=r.company_id AND p.request_id=r.id)
                  THEN '/requests/' || r.id || '/edit' ELSE '' END AS edit_path,
             '' AS extra_path
      FROM purchase_requests r
      WHERE r.company_id=?
      UNION ALL
      SELECT 'COMPRA' AS report_type, p.id AS source_id, p.voucher_code AS voucher_code,
             CASE WHEN p.status='INGRESO_RECHAZADO' THEN 'RECHAZADA' ELSE p.status END AS status,
             p.created_at,
             CASE WHEN p.status='INGRESO_RECHAZADO'
                  THEN 'Rechazo ingreso: ' || COALESCE(NULLIF(TRIM(p.ingress_rejection_note),''), '-')
                  ELSE IFNULL(s.name,'-') END AS note,
             '/purchases/' || p.id || '/pdf' AS pdf_path,
             CASE WHEN p.status NOT IN ('INGRESADA','RECHAZADA','PENDIENTE_APROB_INGRESO') THEN '/purchases/' || p.id || '/edit' ELSE '' END AS edit_path,
             CASE WHEN p.status IN ('APROBADA','INGRESO_RECHAZADO') THEN '/purchases/' || p.id || '/receive' ELSE '' END AS extra_path
      FROM purchases p
      LEFT JOIN suppliers s ON s.id=p.supplier_id
      WHERE p.company_id=?
      UNION ALL
      SELECT 'INGRESO' AS report_type, p.id AS source_id,
             CASE WHEN p.status='INGRESO_RECHAZADO' THEN p.voucher_code ELSE p.receipt_code END AS voucher_code,
             CASE WHEN p.status='INGRESO_RECHAZADO' THEN 'RECHAZADA' ELSE p.status END AS status,
             COALESCE(p.stock_received_at, p.ingress_rejected_at, p.created_at) AS created_at,
             CASE WHEN p.status='INGRESO_RECHAZADO'
                  THEN 'Motivo rechazo: ' || COALESCE(NULLIF(TRIM(p.ingress_rejection_note),''), '-')
                  ELSE IFNULL(d.name,'-') || ' / ' || IFNULL(se.name,'-') END AS note,
             CASE WHEN p.status='INGRESO_RECHAZADO' AND IFNULL(TRIM(p.ingress_doc_pdf),'')!=''
                  THEN p.ingress_doc_pdf
                  WHEN p.status='INGRESO_RECHAZADO' THEN '/purchases/' || p.id || '/pdf'
                  ELSE '/purchases/' || p.id || '/ingress-pdf' END AS pdf_path,
             '' AS edit_path,
             CASE WHEN p.status='INGRESO_RECHAZADO' THEN '/purchases/' || p.id || '/receive' ELSE '' END AS extra_path
      FROM purchases p
      LEFT JOIN deposits d ON d.id=p.deposit_id
      LEFT JOIN sectors se ON se.id=p.sector_id
      WHERE p.company_id=? AND ( (p.receipt_code IS NOT NULL AND TRIM(p.receipt_code)!='') OR p.status='INGRESO_RECHAZADO' )
    ) x
    ${filters.length ? `WHERE ${filters.join(" AND ")}` : ""}
    ORDER BY created_at DESC
    LIMIT 300;`,
    [companyId, companyId, companyId, ...params.slice(1)]
  );
  const tableRows = rows
    .map(
      (r) =>
        `<tr><td>${escapeHtml(r.report_type)}</td><td>${escapeHtml(r.voucher_code || "-")}</td><td>${escapeHtml(
          r.status || "-"
        )}</td><td>${escapeHtml(r.note || "-")}</td><td>${escapeHtml(r.created_at || "-")}</td><td>
        <a class="badge" href="${r.pdf_path}">PDF</a>
        ${r.edit_path ? `<a class="badge" href="${r.edit_path}">Editar</a>` : ""}
        ${r.extra_path ? `<a class="badge" href="${r.extra_path}">${r.status === "RECHAZADA" ? "Reintentar ingreso" : "Ingresar"}</a>` : ""}
        </td></tr>`
    )
    .join("");
  const html = renderAppShell({
    title: "Reportes",
    subtitle: "Consulta de comprobantes por tipo y numero",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "reports",
    body: `<form method="get" action="/reports"><div class="form-grid">
      <select name="type">
        <option value="">Todos</option>
        <option value="SOLICITUD" ${type === "SOLICITUD" ? "selected" : ""}>Solicitudes</option>
        <option value="COMPRA" ${type === "COMPRA" ? "selected" : ""}>Compras</option>
        <option value="INGRESO" ${type === "INGRESO" ? "selected" : ""}>Ingresos</option>
      </select>
      <input name="number" value="${escapeHtml(number)}" placeholder="Numero de comprobante exacto o parcial" />
    </div><button type="submit" class="btn-compact">Filtrar</button></form>
    <table><thead><tr><th>Tipo</th><th>Comprobante</th><th>Estado</th><th>Observacion / Ref.</th><th>Fecha</th><th>Accion</th></tr></thead><tbody>${
      tableRows || "<tr><td colspan='6'>Sin resultados</td></tr>"
    }</tbody></table>`,
  });
  res.send(renderLayout("Reportes", html));
});

app.get("/kardex", requireAuth, requireModule("kardex"), async (req, res) => {
  const { companyId, companyName, username, allowedModules } = req.session.user;
  const from = (req.query.from || "").toString().trim();
  const to = (req.query.to || "").toString().trim();
  const productId = Number(req.query.productId || 0);
  const depositId = Number(req.query.depositId || 0);
  const sectorId = Number(req.query.sectorId || 0);
  const serialQ = (req.query.serialQ || "").toString().trim();
  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";

  const products = await all("SELECT id, name FROM products WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const deposits = await all("SELECT id, name FROM deposits WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const sectors = await all("SELECT id, name, deposit_id FROM sectors WHERE company_id=? AND status='ACTIVO' ORDER BY name;", [companyId]);
  const productOptions = products
    .map((p) => `<option value="${p.id}" ${productId === p.id ? "selected" : ""}>${escapeHtml(p.name)}</option>`)
    .join("");
  const depositOptions = deposits
    .map((d) => `<option value="${d.id}" ${depositId === d.id ? "selected" : ""}>${escapeHtml(d.name)}</option>`)
    .join("");
  const sectorOptions = sectors
    .map(
      (s) =>
        `<option value="${s.id}" data-deposit="${s.deposit_id}" ${sectorId === s.id ? "selected" : ""}>${escapeHtml(
          s.name
        )}</option>`
    )
    .join("");

  const filters = ["sm.company_id=?"];
  const params = [companyId];
  if (from) {
    filters.push("date(sm.created_at) >= date(?)");
    params.push(from);
  }
  if (to) {
    filters.push("date(sm.created_at) <= date(?)");
    params.push(to);
  }
  if (productId) {
    filters.push("sm.product_id=?");
    params.push(productId);
  }
  if (depositId) {
    filters.push("sm.deposit_id=?");
    params.push(depositId);
  }
  if (sectorId) {
    filters.push("sm.sector_id=?");
    params.push(sectorId);
  }
  if (serialQ) {
    filters.push(
      `sm.purchase_id IS NOT NULL AND EXISTS (
        SELECT 1 FROM product_serials ps
        WHERE ps.company_id = sm.company_id
          AND ps.purchase_id = sm.purchase_id
          AND ps.product_id = sm.product_id
          AND ps.serial_number LIKE ?
      )`
    );
    params.push(`%${serialQ}%`);
  }

  const movements = await all(
    `SELECT sm.id, sm.movement_type, sm.quantity, sm.note, sm.created_at,
            p.id AS product_id, p.name AS product_name,
            IFNULL(d.name,'-') AS deposit_name, IFNULL(se.name,'-') AS sector_name,
            IFNULL(sup.name,'-') AS supplier_name,
            IFNULL(pr.request_code,'-') AS request_code,
            IFNULL(pu.voucher_code,'-') AS purchase_code,
            IFNULL(pu.receipt_code,'-') AS ingress_code,
            IFNULL(pu.ingress_doc_type,'-') AS ingress_doc_type,
            IFNULL(pu.ingress_doc_series,'-') AS ingress_doc_series,
            IFNULL(pu.ingress_doc_pdf,'') AS ingress_doc_pdf,
            sm.purchase_id
     FROM stock_movements sm
     JOIN products p ON p.id=sm.product_id
     LEFT JOIN deposits d ON d.id=sm.deposit_id
     LEFT JOIN sectors se ON se.id=sm.sector_id
     LEFT JOIN purchases pu ON pu.id=sm.purchase_id
     LEFT JOIN purchase_requests pr ON pr.id=pu.request_id
     LEFT JOIN suppliers sup ON sup.id=pu.supplier_id
     WHERE ${filters.join(" AND ")}
     ORDER BY sm.id DESC
     LIMIT 300;`,
    params
  );

  const purchaseIds = Array.from(new Set(movements.map((m) => m.purchase_id).filter(Boolean)));
  let serialRows = [];
  if (purchaseIds.length) {
    const placeholders = purchaseIds.map(() => "?").join(",");
    serialRows = await all(
      `SELECT purchase_id, product_id, serial_number
       FROM product_serials
       WHERE company_id=? AND purchase_id IN (${placeholders})
       ORDER BY purchase_id, product_id, serial_number;`,
      [companyId, ...purchaseIds]
    );
  }
  const serialKeyMap = new Map();
  for (const s of serialRows) {
    const key = `${s.purchase_id}:${s.product_id}`;
    if (!serialKeyMap.has(key)) serialKeyMap.set(key, []);
    serialKeyMap.get(key).push(s.serial_number);
  }

  const rows = movements
    .map((m) => {
      const serials = serialKeyMap.get(`${m.purchase_id}:${m.product_id}`) || [];
      const serialsSorted = [...serials].sort((a, b) => String(a).localeCompare(String(b), undefined, { sensitivity: "base" }));
      const serialB64 = Buffer.from(JSON.stringify(serialsSorted)).toString("base64");
      const serialAttr = encodeURIComponent(serialB64);
      const seriesCell =
        serialsSorted.length === 0
          ? "-"
          : `<button type="button" class="btn-compact k-serial-btn" data-serials-b64="${escapeHtml(
              serialAttr
            )}">Ver series (${serialsSorted.length})</button>`;
      const pdfCell =
        m.ingress_doc_pdf && String(m.ingress_doc_pdf).trim()
          ? `<a class="badge" href="${escapeHtml(m.ingress_doc_pdf)}" target="_blank" rel="noopener">PDF</a>`
          : "-";
      return `<tr>
        <td>${m.id}</td>
        <td>${escapeHtml(m.created_at)}</td>
        <td>${escapeHtml(m.product_name)}</td>
        <td>${escapeHtml(m.movement_type)}</td>
        <td>${m.quantity}</td>
        <td>${escapeHtml(m.deposit_name)}</td>
        <td>${escapeHtml(m.sector_name)}</td>
        <td>${escapeHtml(m.supplier_name)}</td>
        <td>${escapeHtml(m.request_code)}</td>
        <td>${escapeHtml(m.purchase_code)}</td>
        <td>${escapeHtml(m.ingress_code)}</td>
        <td>${escapeHtml(m.ingress_doc_type)}</td>
        <td>${escapeHtml(m.ingress_doc_series)}</td>
        <td>${pdfCell}</td>
        <td>${seriesCell}</td>
      </tr>`;
    })
    .join("");

  const html = renderAppShell({
    title: "Kardex",
    subtitle: "Trazabilidad detallada por producto/ubicacion/series",
    companyName,
    username,
    allowedModules,
    activeGroup: "logistics",
    activeSection: "kardex",
    body: `${ok}${error}
      <form method="get" action="/kardex">
        <div class="form-grid">
          <select name="productId"><option value="">Producto</option>${productOptions}</select>
          <select name="depositId" id="kDeposit"><option value="">Deposito</option>${depositOptions}</select>
          <select name="sectorId" id="kSector"><option value="">Sector</option>${sectorOptions}</select>
          <input name="serialQ" value="${escapeHtml(serialQ)}" placeholder="Serie (contiene)" />
          <input type="date" name="from" value="${escapeHtml(from)}" />
          <input type="date" name="to" value="${escapeHtml(to)}" />
        </div>
        <button type="submit" class="btn-compact">Filtrar Kardex</button>
      </form>
      <table><thead><tr>
        <th>ID</th><th>Fecha</th><th>Producto</th><th>Tipo</th><th>Cant</th><th>Deposito</th><th>Sector</th><th>Proveedor</th><th>Solicitud</th><th>Compra</th><th>Ingreso</th><th>T.Doc</th><th>Serie doc.</th><th>PDF</th><th>Series</th>
      </tr></thead><tbody>${rows || "<tr><td colspan='15'>Sin movimientos</td></tr>"}</tbody></table>
      <div id="kSerialBackdrop" style="display:none;position:fixed;inset:0;background:rgba(15,23,42,.55);z-index:90;"></div>
      <div id="kSerialModal" style="display:none;position:fixed;inset:0;z-index:91;align-items:center;justify-content:center;padding:28px;">
        <div style="width:min(520px,96vw);background:#fff;border-radius:14px;box-shadow:0 24px 80px rgba(15,23,42,.35);overflow:hidden;">
          <div style="display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid #e5e7eb;">
            <div style="font-weight:800;">Series del movimiento</div>
            <button type="button" class="btn-compact" id="kSerialClose">Cerrar</button>
          </div>
          <div style="padding:12px 16px 16px;max-height:70vh;overflow:auto;">
            <table style="width:100%;"><thead><tr><th style="width:72px;">N°</th><th>Serie</th></tr></thead><tbody id="kSerialTbody"></tbody></table>
          </div>
        </div>
      </div>
      <script>
        (function(){
          const dep = document.getElementById('kDeposit');
          const sec = document.getElementById('kSector');
          function apply(){
            const d = dep?.value || '';
            sec?.querySelectorAll('option[data-deposit]')?.forEach(o => { o.hidden = d && o.dataset.deposit !== d; });
          }
          dep?.addEventListener('change', apply);
          apply();
        })();
        (function(){
          const backdrop = document.getElementById('kSerialBackdrop');
          const modal = document.getElementById('kSerialModal');
          const tbody = document.getElementById('kSerialTbody');
          const closeBtn = document.getElementById('kSerialClose');
          function closeM(){ if (backdrop) backdrop.style.display='none'; if (modal) modal.style.display='none'; }
          function openM(list){
            if (!tbody || !modal || !backdrop) return;
            tbody.innerHTML = (list||[]).map((s,i)=>'<tr><td>'+(i+1)+'</td><td>'+String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')+'</td></tr>').join('');
            backdrop.style.display='block';
            modal.style.display='flex';
          }
          closeBtn?.addEventListener('click', closeM);
          backdrop?.addEventListener('click', closeM);
          document.body.addEventListener('click', function(ev){
            const btn = ev.target && ev.target.closest && ev.target.closest('.k-serial-btn');
            if (!btn) return;
            ev.preventDefault();
            const enc = btn.getAttribute('data-serials-b64') || '';
            try {
              const b64 = decodeURIComponent(enc);
              const list = JSON.parse(atob(b64));
              openM(Array.isArray(list) ? list : []);
            } catch (e) { openM([]); }
          });
        })();
      </script>`,
  });
  res.send(renderLayout("Kardex", html));
});

app.get("/profiles", requireAuth, requireModule("profiles"), async (req, res) => {
  const { companyId, companyName, allowedModules } = req.session.user;
  await ensureCompanyProfiles(companyId);

  const ok = req.query.ok ? `<div class="ok">${escapeHtml(req.query.ok)}</div>` : "";
  const error = req.query.error ? `<div class="error">${escapeHtml(req.query.error)}</div>` : "";
  const editId = req.query.editId ? Number(req.query.editId) : null;

  const profiles = await all(
    "SELECT id, name, description FROM profiles WHERE company_id = ? ORDER BY id DESC;",
    [companyId]
  );
  let editProfile = null;
  let editAccessMap = {};
  if (editId) {
    editProfile = await get("SELECT id, name, description FROM profiles WHERE id = ? AND company_id = ?;", [
      editId,
      companyId,
    ]);
    if (editProfile) {
      const accessRows = await all(
        "SELECT module_key, can_access FROM profile_modules WHERE profile_id = ?;",
        [editId]
      );
      editAccessMap = Object.fromEntries(accessRows.map((row) => [row.module_key, row.can_access === 1]));
    }
  }

  const rows = profiles
    .map(
      (p) => `<tr>
      <td>${p.id}</td>
      <td>${escapeHtml(p.name)}</td>
      <td>${escapeHtml(p.description || "-")}</td>
      <td><a href="/profiles?editId=${p.id}">Editar</a></td>
    </tr>`
    )
    .join("");

  const formAction = editProfile ? `/profiles/${editProfile.id}` : "/profiles";
  const formTitle = editProfile ? "Editar perfil" : "Crear perfil";
  const buttonText = editProfile ? "Actualizar perfil" : "Guardar perfil";
  const nameValue = editProfile ? escapeHtml(editProfile.name) : "";
  const descriptionValue = editProfile ? escapeHtml(editProfile.description || "") : "";

  const moduleChecks = MODULES.map((moduleDef) => {
    const checked = editProfile
      ? editAccessMap[moduleDef.key]
        ? "checked"
        : ""
      : moduleDef.key === "dashboard"
      ? "checked"
      : "";
    return `<label style="display:flex; align-items:center; gap:8px;">
      <input type="checkbox" name="module_${moduleDef.key}" value="1" ${checked} style="width:auto;" />
      ${moduleDef.label}
    </label>`;
  }).join("");

  const html = renderAppShell({
    title: "Perfiles y Permisos",
    subtitle: "Define ventanas habilitadas por perfil",
    companyName,
    username: req.session.user.username,
    allowedModules,
    activeGroup: "user-management",
    activeSection: "profiles",
    body: `
      ${ok}
      ${error}
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <h3>${formTitle}</h3>
        <a href="/exports/profiles" class="badge">Exportar Excel</a>
      </div>
      <form method="post" action="${formAction}">
        <div class="form-grid">
          <input name="name" value="${nameValue}" placeholder="Nombre del perfil" required />
          <input name="description" value="${descriptionValue}" placeholder="Descripcion" />
        </div>
        <div class="form-grid">
          ${moduleChecks}
        </div>
        <button type="submit">${buttonText}</button>
      </form>
      <h3 style="margin-top:18px">Perfiles de la empresa</h3>
      <table>
        <thead><tr><th>ID</th><th>Nombre</th><th>Descripcion</th><th>Accion</th></tr></thead>
        <tbody>${rows || "<tr><td colspan='4'>Sin perfiles</td></tr>"}</tbody>
      </table>`,
  });
  res.send(renderLayout("Perfiles", html));
});

app.post("/profiles", requireAuth, requireModule("profiles"), async (req, res) => {
  const { companyId } = req.session.user;
  const { name, description } = req.body;
  if (!name || !name.trim()) {
    return res.redirect("/profiles?error=Nombre+de+perfil+obligatorio");
  }
  try {
    await run("INSERT INTO profiles(company_id, name, description) VALUES (?, ?, ?);", [
      companyId,
      name.trim(),
      (description || "").trim(),
    ]);
  } catch {
    return res.redirect("/profiles?error=Ya+existe+un+perfil+con+ese+nombre");
  }
  const newProfile = await get(
    "SELECT id FROM profiles WHERE company_id = ? AND name = ?;",
    [companyId, name.trim()]
  );
  for (const moduleDef of MODULES) {
    const canAccess = req.body[`module_${moduleDef.key}`] ? 1 : 0;
    await run(
      "INSERT OR IGNORE INTO profile_modules(profile_id, module_key, can_access) VALUES (?, ?, ?);",
      [newProfile.id, moduleDef.key, canAccess]
    );
    await run(
      "UPDATE profile_modules SET can_access = ? WHERE profile_id = ? AND module_key = ?;",
      [canAccess, newProfile.id, moduleDef.key]
    );
  }
  return res.redirect("/profiles?ok=Perfil+creado+correctamente");
});

app.post("/profiles/:id", requireAuth, requireModule("profiles"), async (req, res) => {
  const { companyId } = req.session.user;
  const profileId = Number(req.params.id);
  const { name, description } = req.body;

  const profile = await get("SELECT id FROM profiles WHERE id = ? AND company_id = ?;", [profileId, companyId]);
  if (!profile) {
    return res.redirect("/profiles?error=Perfil+no+encontrado");
  }

  try {
    await run("UPDATE profiles SET name = ?, description = ? WHERE id = ? AND company_id = ?;", [
      name.trim(),
      (description || "").trim(),
      profileId,
      companyId,
    ]);
  } catch {
    return res.redirect(`/profiles?editId=${profileId}&error=Nombre+de+perfil+duplicado`);
  }

  for (const moduleDef of MODULES) {
    const canAccess = req.body[`module_${moduleDef.key}`] ? 1 : 0;
    await run(
      "INSERT OR IGNORE INTO profile_modules(profile_id, module_key, can_access) VALUES (?, ?, ?);",
      [profileId, moduleDef.key, canAccess]
    );
    await run(
      "UPDATE profile_modules SET can_access = ? WHERE profile_id = ? AND module_key = ?;",
      [canAccess, profileId, moduleDef.key]
    );
  }
  return res.redirect("/profiles?ok=Perfil+actualizado+correctamente");
});

app.get("/exports/:entity", requireAuth, async (req, res) => {
  const { entity } = req.params;
  const { companyId } = req.session.user;
  const from = req.query.from || "";
  const to = req.query.to || "";

  const dateFilterFor = (columnName) => {
    const filters = [];
    const params = [companyId];
    if (from) {
      filters.push(`date(${columnName}) >= date(?)`);
      params.push(from);
    }
    if (to) {
      filters.push(`date(${columnName}) <= date(?)`);
      params.push(to);
    }
    return {
      where: filters.length > 0 ? ` AND ${filters.join(" AND ")}` : "",
      params,
    };
  };

  if (entity === "workers") {
    if (!req.session.user.allowedModules.includes("workers")) return res.status(403).send("Sin permisos");
    const rows = await all(
      "SELECT id, first_name || ' ' || last_name AS nombre, document_type || ' ' || document_number AS documento, phone, email, status FROM workers WHERE company_id=? ORDER BY id DESC;",
      [companyId]
    );
    return sendCsv(
      res,
      "trabajadores.csv",
      [
        { key: "id", label: "ID" },
        { key: "nombre", label: "Nombre" },
        { key: "documento", label: "Documento" },
        { key: "phone", label: "Telefono" },
        { key: "email", label: "Correo" },
        { key: "status", label: "Estado" },
      ],
      rows
    );
  }

  if (entity === "users") {
    if (!req.session.user.allowedModules.includes("users")) return res.status(403).send("Sin permisos");
    const rows = await all(
      `SELECT u.id, u.username, IFNULL(p.name,u.role) AS perfil, IFNULL(w.first_name || ' ' || w.last_name,'-') AS trabajador, u.status
       FROM users u
       LEFT JOIN profiles p ON p.id=u.profile_id
       LEFT JOIN workers w ON w.id=u.worker_id
       WHERE u.company_id=? ORDER BY u.id DESC;`,
      [companyId]
    );
    return sendCsv(
      res,
      "usuarios.csv",
      [
        { key: "id", label: "ID" },
        { key: "username", label: "Username" },
        { key: "perfil", label: "Perfil" },
        { key: "trabajador", label: "Trabajador" },
        { key: "status", label: "Estado" },
      ],
      rows
    );
  }

  if (entity === "profiles") {
    if (!req.session.user.allowedModules.includes("profiles")) return res.status(403).send("Sin permisos");
    const rows = await all("SELECT id, name, description, created_at FROM profiles WHERE company_id=? ORDER BY id DESC;", [
      companyId,
    ]);
    return sendCsv(
      res,
      "perfiles.csv",
      [
        { key: "id", label: "ID" },
        { key: "name", label: "Perfil" },
        { key: "description", label: "Descripcion" },
        { key: "created_at", label: "Fecha" },
      ],
      rows
    );
  }

  if (entity === "brands") {
    if (!req.session.user.allowedModules.includes("brands")) return res.status(403).send("Sin permisos");
    const rows = await all("SELECT id, name, status, created_at FROM brands WHERE company_id=? ORDER BY id DESC;", [companyId]);
    return sendCsv(
      res,
      "marcas.csv",
      [
        { key: "id", label: "ID" },
        { key: "name", label: "Marca" },
        { key: "status", label: "Estado" },
        { key: "created_at", label: "Fecha" },
      ],
      rows
    );
  }

  if (entity === "categories") {
    if (!req.session.user.allowedModules.includes("categories")) return res.status(403).send("Sin permisos");
    const rows = await all("SELECT id, name, status, created_at FROM categories WHERE company_id=? ORDER BY id DESC;", [
      companyId,
    ]);
    return sendCsv(
      res,
      "categorias.csv",
      [
        { key: "id", label: "ID" },
        { key: "name", label: "Categoria" },
        { key: "status", label: "Estado" },
        { key: "created_at", label: "Fecha" },
      ],
      rows
    );
  }

  if (entity === "suppliers") {
    if (!req.session.user.allowedModules.includes("suppliers")) return res.status(403).send("Sin permisos");
    const rows = await all(
      "SELECT id, name, ruc, contact_name, phone, email, status, created_at FROM suppliers WHERE company_id=? ORDER BY id DESC;",
      [companyId]
    );
    return sendCsv(
      res,
      "proveedores.csv",
      [
        { key: "id", label: "ID" },
        { key: "name", label: "Proveedor" },
        { key: "ruc", label: "RUC" },
        { key: "contact_name", label: "Contacto" },
        { key: "phone", label: "Telefono" },
        { key: "email", label: "Correo" },
        { key: "status", label: "Estado" },
        { key: "created_at", label: "Fecha" },
      ],
      rows
    );
  }

  if (entity === "products") {
    if (!req.session.user.allowedModules.includes("products")) return res.status(403).send("Sin permisos");
    const rows = await all(
      `SELECT p.id, p.name, IFNULL(c.name,'-') AS categoria, IFNULL(b.name,'-') AS marca,
              p.model, p.current_stock, p.reorder_level, p.requires_serial, p.status
       FROM products p
       LEFT JOIN categories c ON c.id = p.category_id
       LEFT JOIN brands b ON b.id = p.brand_id
       WHERE p.company_id=? ORDER BY p.id DESC;`,
      [companyId]
    );
    return sendCsv(
      res,
      "productos.csv",
      [
        { key: "id", label: "ID" },
        { key: "name", label: "Producto" },
        { key: "categoria", label: "Categoria" },
        { key: "marca", label: "Marca" },
        { key: "model", label: "Modelo" },
        { key: "current_stock", label: "Stock" },
        { key: "reorder_level", label: "Stock Minimo" },
        { key: "requires_serial", label: "Requiere Serie" },
        { key: "status", label: "Estado" },
      ],
      rows
    );
  }

  if (entity === "deposits") {
    if (!req.session.user.allowedModules.includes("deposits")) return res.status(403).send("Sin permisos");
    const rows = await all("SELECT id, name, status, created_at FROM deposits WHERE company_id=? ORDER BY id DESC;", [companyId]);
    return sendCsv(
      res,
      "depositos.csv",
      [
        { key: "id", label: "ID" },
        { key: "name", label: "Deposito" },
        { key: "status", label: "Estado" },
        { key: "created_at", label: "Fecha" },
      ],
      rows
    );
  }

  if (entity === "sectors") {
    if (!req.session.user.allowedModules.includes("sectors")) return res.status(403).send("Sin permisos");
    const rows = await all(
      `SELECT s.id, s.name, IFNULL(d.name,'-') AS deposito, s.status, s.created_at
       FROM sectors s LEFT JOIN deposits d ON d.id=s.deposit_id
       WHERE s.company_id=? ORDER BY s.id DESC;`,
      [companyId]
    );
    return sendCsv(
      res,
      "sectores.csv",
      [
        { key: "id", label: "ID" },
        { key: "name", label: "Sector" },
        { key: "deposito", label: "Deposito" },
        { key: "status", label: "Estado" },
        { key: "created_at", label: "Fecha" },
      ],
      rows
    );
  }

  if (entity === "stock") {
    if (!req.session.user.allowedModules.includes("stock")) return res.status(403).send("Sin permisos");
    const d = dateFilterFor("sm.created_at");
    const rows = await all(
      `SELECT sm.id, p.name AS producto, sm.movement_type, sm.quantity, sm.created_at,
              IFNULL(d.name,'-') AS deposito, IFNULL(se.name,'-') AS sector, IFNULL(sup.name,'-') AS proveedor,
              IFNULL(pu.voucher_code,'-') AS comprobante,
              IFNULL(pu.receipt_code,'-') AS ingreso,
              IFNULL(pu.ingress_doc_type,'-') AS tipo_doc_ingreso,
              IFNULL(pu.ingress_doc_series,'-') AS serie_doc_ingreso,
              IFNULL(pu.ingress_doc_pdf,'') AS pdf_ingreso
       FROM stock_movements sm
       JOIN products p ON p.id=sm.product_id
       LEFT JOIN deposits d ON d.id=sm.deposit_id
       LEFT JOIN sectors se ON se.id=sm.sector_id
       LEFT JOIN purchases pu ON pu.id=sm.purchase_id
       LEFT JOIN suppliers sup ON sup.id=sm.supplier_id
       WHERE sm.company_id=? ${d.where}
       ORDER BY sm.id DESC;`,
      d.params
    );
    return sendCsv(
      res,
      "stock_movimientos.csv",
      [
        { key: "id", label: "ID" },
        { key: "producto", label: "Producto" },
        { key: "movement_type", label: "Tipo" },
        { key: "quantity", label: "Cantidad" },
        { key: "deposito", label: "Deposito" },
        { key: "sector", label: "Sector" },
        { key: "proveedor", label: "Proveedor" },
        { key: "comprobante", label: "Comprobante compra" },
        { key: "ingreso", label: "Correlativo ingreso" },
        { key: "tipo_doc_ingreso", label: "Tipo doc. ingreso" },
        { key: "serie_doc_ingreso", label: "Serie doc. ingreso" },
        { key: "pdf_ingreso", label: "PDF ingreso (ruta)" },
        { key: "created_at", label: "Fecha" },
      ],
      rows
    );
  }

  if (entity === "prices") {
    if (!req.session.user.allowedModules.includes("prices")) return res.status(403).send("Sin permisos");
    const d = dateFilterFor("pp.created_at");
    const rows = await all(
      `SELECT pp.id, p.name AS producto, pp.price, pp.effective_date, pp.note, pp.created_at
       FROM product_prices pp
       JOIN products p ON p.id=pp.product_id
       WHERE pp.company_id=? ${d.where}
       ORDER BY pp.id DESC;`,
      d.params
    );
    return sendCsv(
      res,
      "historial_precios.csv",
      [
        { key: "id", label: "ID" },
        { key: "producto", label: "Producto" },
        { key: "price", label: "Precio" },
        { key: "effective_date", label: "Vigencia" },
        { key: "note", label: "Nota" },
        { key: "created_at", label: "Fecha" },
      ],
      rows
    );
  }

  if (entity === "requests") {
    if (!req.session.user.allowedModules.includes("requests")) return res.status(403).send("Sin permisos");
    const rows = await all(
      `SELECT r.request_code, IFNULL(COUNT(ri.id),0) AS items, IFNULL(SUM(ri.quantity),0) AS quantity, r.status,
              u.username AS solicitante, IFNULL(au.username,'-') AS aprobador, r.note, r.created_at
       FROM purchase_requests r
       JOIN users u ON u.id=r.requested_by_user_id
       LEFT JOIN users au ON au.id=r.approved_by_user_id
       LEFT JOIN request_items ri ON ri.request_id=r.id
       WHERE r.company_id=?
       GROUP BY r.id
       ORDER BY r.id DESC;`,
      [companyId]
    );
    return sendCsv(
      res,
      "solicitudes.csv",
      [
        { key: "request_code", label: "Codigo" },
        { key: "items", label: "Items" },
        { key: "quantity", label: "Cantidad" },
        { key: "status", label: "Estado" },
        { key: "solicitante", label: "Solicitante" },
        { key: "aprobador", label: "Aprobador" },
        { key: "note", label: "Nota" },
        { key: "created_at", label: "Fecha" },
      ],
      rows
    );
  }

  if (entity === "purchases") {
    if (!req.session.user.allowedModules.includes("purchases")) return res.status(403).send("Sin permisos");
    const rows = await all(
      `SELECT p.voucher_code, p.status, IFNULL(COUNT(pi.id),0) AS items, IFNULL(SUM(pi.quantity),0) AS quantity,
              p.total, IFNULL(s.name,'-') AS proveedor, IFNULL(pr.request_code,'-') AS solicitud, IFNULL(p.receipt_code,'-') AS ingreso, p.created_at
       FROM purchases p
       LEFT JOIN purchase_items pi ON pi.purchase_id=p.id
       LEFT JOIN suppliers s ON s.id=p.supplier_id
       LEFT JOIN purchase_requests pr ON pr.id=p.request_id
       WHERE p.company_id=?
       GROUP BY p.id
       ORDER BY p.id DESC;`,
      [companyId]
    );
    return sendCsv(
      res,
      "compras.csv",
      [
        { key: "voucher_code", label: "Comprobante" },
        { key: "status", label: "Estado" },
        { key: "items", label: "Items" },
        { key: "quantity", label: "Cantidad" },
        { key: "unit_price", label: "Precio Unitario" },
        { key: "total", label: "Total" },
        { key: "proveedor", label: "Proveedor" },
        { key: "solicitud", label: "Solicitud" },
        { key: "ingreso", label: "Comprobante Ingreso" },
        { key: "created_at", label: "Fecha" },
      ],
      rows
    );
  }

  if (entity === "approvers") {
    if (!req.session.user.allowedModules.includes("approvers")) return res.status(403).send("Sin permisos");
    const rows = await all(
      `SELECT u.id, u.username, a.status, a.created_at
       FROM approvers a
       JOIN users u ON u.id = a.user_id
       WHERE a.company_id = ?
       ORDER BY u.username;`,
      [companyId]
    );
    return sendCsv(
      res,
      "aprobadores.csv",
      [
        { key: "id", label: "ID Usuario" },
        { key: "username", label: "Usuario" },
        { key: "status", label: "Estado" },
        { key: "created_at", label: "Fecha" },
      ],
      rows
    );
  }

  return res.status(404).send("Export no disponible");
});

async function start() {
  await ensureSchema();
  await ensureBootstrapDemoUsers();
  app.listen(PORT, () => {
    console.log(`Servidor activo en http://localhost:${PORT}`);
  });
}

start().catch((error) => {
  console.error("No se pudo iniciar la aplicacion:", error);
  process.exit(1);
});
