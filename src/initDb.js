const { run, get, all, DB_PATH } = require("./db");

const companies = [
  "ALLIN GROUP - JAVIER PRADO S.A.",
  "CONSORCIO VIA",
  "INVERSIONES Y REPRESENTACIONES POLO S.A.C.",
  "EMP. TRANSPORTES Y SERVICIOS SANTA ROSA DE LIMA",
  "POLO MINERIA Y PROYECTOS SAC",
  "PERU BUS INTERNACIONAL S.A.",
  "POLO MANTENIMIENTO Y SERVICIOS S.A.C",
  "GESTION DE TRANSPORTE INTEGRAL",
  "VILLA SILVESTRE SAC",
  "FUNDO SILVESTRE SAC",
  "EMPRESA DE TRANSPORTES Y SERVICIOS LOS ALIZOS S.A.",
  "NUEVO HORIZONTE VERDE S.A.C",
];

async function bootstrap() {
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
    // Columna ya existe en bases previamente creadas.
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
    // Columna ya existe.
  }

  await run(`
    CREATE TABLE IF NOT EXISTS claims (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT NOT NULL UNIQUE,
      customer_name TEXT NOT NULL,
      product TEXT NOT NULL,
      issue TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'Abierto',
      company_id INTEGER NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id)
    );
  `);

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
      FOREIGN KEY(sector_id) REFERENCES sectors(id),
      UNIQUE(company_id, serial_number)
    );
  `);

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

  for (const companyName of companies) {
    await run(
      "INSERT OR IGNORE INTO companies(name, status) VALUES(?, 'ACT');",
      [companyName]
    );
  }

  const adminCompany = await get(
    "SELECT id FROM companies WHERE name = ?;",
    [companies[0]]
  );

  const moduleKeys = [
    "dashboard",
    "workers",
    "users",
    "profiles",
    "brands",
    "products",
    "stock",
    "suppliers",
    "categories",
    "requests",
    "purchases",
    "approvers",
    "approvals_requests",
    "approvals_purchases",
    "deposits",
    "sectors",
    "ingresses",
    "approvals_ingresses",
    "reports",
    "kardex",
  ];

  async function seedProfilesAndModulesForCompany(companyId) {
    await run(
      "INSERT OR IGNORE INTO profiles(company_id, name, description) VALUES (?, ?, ?);",
      [companyId, "Administrador", "Acceso total a todos los modulos"]
    );
    await run(
      "INSERT OR IGNORE INTO profiles(company_id, name, description) VALUES (?, ?, ?);",
      [companyId, "Supervisor", "Acceso a operaciones y lectura de usuarios"]
    );
    await run(
      "INSERT OR IGNORE INTO profiles(company_id, name, description) VALUES (?, ?, ?);",
      [companyId, "Operador", "Acceso solo a dashboard"]
    );
    const adminProfile = await get(
      "SELECT id FROM profiles WHERE company_id = ? AND name = 'Administrador';",
      [companyId]
    );
    const supervisorProfile = await get(
      "SELECT id FROM profiles WHERE company_id = ? AND name = 'Supervisor';",
      [companyId]
    );
    const operatorProfile = await get(
      "SELECT id FROM profiles WHERE company_id = ? AND name = 'Operador';",
      [companyId]
    );
    for (const key of moduleKeys) {
      await run(
        "INSERT OR IGNORE INTO profile_modules(profile_id, module_key, can_access) VALUES (?, ?, 1);",
        [adminProfile.id, key]
      );
      await run(
        "INSERT OR IGNORE INTO profile_modules(profile_id, module_key, can_access) VALUES (?, ?, ?);",
        [supervisorProfile.id, key, key === "profiles" || key === "approvers" ? 0 : 1]
      );
      await run(
        "INSERT OR IGNORE INTO profile_modules(profile_id, module_key, can_access) VALUES (?, ?, ?);",
        [operatorProfile.id, key, key === "dashboard" ? 1 : 0]
      );
    }
    return adminProfile.id;
  }

  const allCompanies = await all("SELECT id, name FROM companies WHERE status='ACT' ORDER BY id;");
  for (const co of allCompanies) {
    const adminProfileId = await seedProfilesAndModulesForCompany(co.id);
    const loginUsername = co.id === adminCompany.id ? "admin" : `admin_${co.id}`;
    await run(
      "INSERT OR IGNORE INTO users(username, password, company_id, role, profile_id) VALUES(?, ?, ?, 'admin', ?);",
      [loginUsername, "admin123", co.id, adminProfileId]
    );
  }

  await run(
    `UPDATE users SET profile_id = (
       SELECT p.id FROM profiles p WHERE p.company_id = users.company_id AND p.name = 'Administrador' LIMIT 1
     )
     WHERE role = 'admin' AND profile_id IS NULL;`
  );

  const existingClaims = await all(
    "SELECT id FROM claims WHERE company_id = ? LIMIT 1;",
    [adminCompany.id]
  );

  if (existingClaims.length === 0) {
    await run(
      "INSERT INTO claims(code, customer_name, product, issue, status, company_id) VALUES (?, ?, ?, ?, ?, ?);",
      ["PV-0001", "Juan Perez", "Refrigeradora", "No enfria correctamente", "Abierto", adminCompany.id]
    );
    await run(
      "INSERT INTO claims(code, customer_name, product, issue, status, company_id) VALUES (?, ?, ?, ?, ?, ?);",
      ["PV-0002", "Maria Rojas", "Lavadora", "Fuga de agua en centrifugado", "En proceso", adminCompany.id]
    );
  }

  console.log(`Base inicializada correctamente en: ${DB_PATH}`);
  console.log("Acceso ALLIN (empresa principal): usuario admin, contrasena admin123.");
  console.log("Otras empresas activas: usuario admin_<id_empresa> (ej. admin_2), contrasena admin123. Ver IDs en tabla companies.");
}

bootstrap().catch((error) => {
  console.error("Error al inicializar base de datos:", error);
  process.exit(1);
});
