# Sistema Post Venta (Electrodomesticos)

Base inicial liviana para servidor limitado:

- Backend `Node.js + Express`
- Base de datos `SQLite` usando `sql.js` (sin compilacion nativa)
- Multiempresa con aislamiento de datos por `company_id`
- Login con seleccion de empresa
- Dashboard inicial con casos post venta
- Modulo `Trabajadores` por empresa
- Modulo `Usuarios` separado, creando credenciales desde trabajador
- `username` unico global (no se repite entre empresas)

## 1) Instalar

```bash
npm install
```

## 2) Crear base inicial

```bash
npm run init-db
```

## 3) Ejecutar

```bash
npm start
```

Abrir: `http://localhost:3000`

Usuario demo:

- Usuario: `admin`
- Contrasena: `admin123`
- Empresa: `ALLIN GROUP - JAVIER PRADO S.A.`

## Notas de arquitectura

- Cada registro de post venta pertenece a una empresa (`company_id`).
- Todas las consultas del dashboard filtran por la empresa de la sesion.
- No hay cruce de informacion entre empresas.
- Los trabajadores son por empresa y se guardan en `workers`.
- El usuario de acceso se crea desde un trabajador en el modulo `Usuarios`.
