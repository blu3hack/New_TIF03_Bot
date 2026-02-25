const fs = require('fs');
const fastcsv = require('fast-csv');
const connectionPromise = require('./connection');

async function deleteData(table) {
  const connection = await connectionPromise;
  const sql = `DELETE FROM \`${table}\``;
  await connection.execute(sql);
}

async function importCSV(table, fileName) {
  const connection = await connectionPromise;
  const filePath = path.join(__dirname, 'loaded_file/download_fulfillment', `${fileName}.csv`);
  const selectedCols = ['INCIDENT', 'WORKZONE', 'WITEL', 'COMPLY_6_E2E', 'COMPLY_36_E2E'];
  const rows = [];

  await new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(fastcsv.parse({ headers: true, trim: true }))
      .on('error', reject)
      .on('data', (row) => {
        rows.push(selectedCols.map((col) => row[col] ?? null));
      })
      .on('end', resolve);
  });

  if (rows.length === 0) {
    console.log('CSV kosong');
    return;
  }

  const batchSize = 300;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const placeholders = batch.map(() => `(${selectedCols.map(() => '?').join(',')})`).join(',');
    const sql = `
      INSERT INTO \`${table}\` (${selectedCols.join(',')})
      VALUES ${placeholders}
    `;
    await connection.execute(sql, batch.flat());
  }
  console.log(`File ${fileName} berhasil diimport ke ${table}`);
}

async function main() {
  try {
    await deleteData('helper_ttr_reseller');
    await importCSV('helper_ttr_reseller', "REPORT_TIKET_COMPLIANCE24_'_'TERRITORY 3'_''_'RESELLER_-");
  } catch (err) {
    console.error('ERROR:', err.message);
  } finally {
    const connection = await connectionPromise;
    await connection.end();
  }
}
main();
