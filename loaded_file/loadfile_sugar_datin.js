const xlsx = require('xlsx');
const fs = require('fs');
const mysql = require('mysql2/promise');
const path = require('path');

async function loadExcelToMySQL() {
  const excelFile = path.join(__dirname, 'download_fulfillment', 'data.xlsx');
  const csvFile = 'temp.csv';
  const tableName = 'incident_report';

  // === 1. Convert Excel ke CSV ===
  const workbook = xlsx.readFile(excelFile);
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];

  const csv = xlsx.utils.sheet_to_csv(worksheet);
  fs.writeFileSync(csvFile, csv);

  // === 2. Koneksi MySQL ===
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'nama_database',
    localInfile: true, // WAJIB
  });

  // === 3. LOAD DATA ===
  const loadQuery = `
        LOAD DATA LOCAL INFILE '${path.resolve(csvFile)}'
        INTO TABLE ${tableName}
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
    `;

  await connection.query(loadQuery);

  await connection.end();

  // === 4. Hapus file CSV sementara ===
  fs.unlinkSync(csvFile);

  console.log('LOAD DATA selesai (sangat cepat)');
}

loadExcelToMySQL().catch((err) => {
  console.error(err);
});
