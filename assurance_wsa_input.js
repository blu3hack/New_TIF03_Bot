const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const connection = require('./connection');
const { insertDate } = require('./currentDate');

// ================= DELETE EXISTING =================
function deleteExistingData() {
  const tables = ['wsa_sugar', 'wsa_service', 'wsa_ttr3', 'wsa_ttr6', 'wsa_ttr36', 'wsa_ttrmanja'];
  const currentDate = insertDate;

  return Promise.all(tables.map((table) => connection.execute(`DELETE FROM ${table} WHERE tgl = ?`, [currentDate])));
}

// ================= DELETE UNWANTED =================
async function deleteUnwantedRows(table) {
  await connection.execute(`DELETE FROM ${table} WHERE witel IN ('TOTAL','TARGET')`);
}

// ================= INPUT CSV =================
function inputDataToDatabase(file, jenis, table) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'loaded_file/wsa_gamas', `${file}.csv`);
    const rows = [];
    const tgl = insertDate;

    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => {
        const values = Object.values(data);
        const newWitel = (values[0] || '').replace('TERITORY', 'TERRITORY');

        rows.push([tgl, jenis, newWitel, ...values.slice(1, 13)]);
      })
      .on('end', async () => {
        try {
          if (rows.length === 0) return resolve();

          const sql = `
            INSERT INTO ${table}
            (tgl, lokasi, witel, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12)
            VALUES ?
          `;

          await connection.query(sql, [rows]);

          console.log(`✅ ${file} berhasil diinput ke ${table}`);

          await deleteUnwantedRows(table);

          resolve();
        } catch (err) {
          reject(err);
        }
      })
      .on('error', reject);
  });
}

// ================= RUN =================
async function run() {
  try {
    console.log('🔄 Menghapus data lama...');
    await deleteExistingData();

    const files = [
      ['service_tif', 'tif', 'wsa_service'],
      ['service_area_ccm', 'area_ccm', 'wsa_service'],
      ['service_balnus_ccm', 'balnus_ccm', 'wsa_service'],
      ['service_jateng_ccm', 'jateng_ccm', 'wsa_service'],
      ['service_jatim_ccm', 'jatim_ccm', 'wsa_service'],
      ['sugar_tif', 'tif', 'wsa_sugar'],
      ['sugar_area_ccm', 'area_ccm', 'wsa_sugar'],
      ['sugar_balnus_ccm', 'balnus_ccm', 'wsa_sugar'],
      ['sugar_jateng_ccm', 'jateng_ccm', 'wsa_sugar'],
      ['sugar_jatim_ccm', 'jatim_ccm', 'wsa_sugar'],
      ['ttr3_tif', 'tif', 'wsa_ttr3'],
      ['ttr3_area_ccm', 'area_ccm', 'wsa_ttr3'],
      ['ttr3_balnus_ccm', 'balnus_ccm', 'wsa_ttr3'],
      ['ttr3_jateng_ccm', 'jateng_ccm', 'wsa_ttr3'],
      ['ttr3_jatim_ccm', 'jatim_ccm', 'wsa_ttr3'],
      ['ttr6_tif', 'tif', 'wsa_ttr6'],
      ['ttr6_area_ccm', 'area_ccm', 'wsa_ttr6'],
      ['ttr6_balnus_ccm', 'balnus_ccm', 'wsa_ttr6'],
      ['ttr6_jateng_ccm', 'jateng_ccm', 'wsa_ttr6'],
      ['ttr6_jatim_ccm', 'jatim_ccm', 'wsa_ttr6'],
      ['ttr36_tif', 'tif', 'wsa_ttr36'],
      ['ttr36_area_ccm', 'area_ccm', 'wsa_ttr36'],
      ['ttr36_balnus_ccm', 'balnus_ccm', 'wsa_ttr36'],
      ['ttr36_jateng_ccm', 'jateng_ccm', 'wsa_ttr36'],
      ['ttr36_jatim_ccm', 'jatim_ccm', 'wsa_ttr36'],
      ['ttrmanja_tif', 'tif', 'wsa_ttrmanja'],
      ['ttrmanja_area_ccm', 'area_ccm', 'wsa_ttrmanja'],
      ['ttrmanja_balnus_ccm', 'balnus_ccm', 'wsa_ttrmanja'],
      ['ttrmanja_jateng_ccm', 'jateng_ccm', 'wsa_ttrmanja'],
      ['ttrmanja_jatim_ccm', 'jatim_ccm', 'wsa_ttrmanja'],
    ];

    for (const fileData of files) {
      await inputDataToDatabase(...fileData);
    }

    console.log('🎉 Semua proses selesai.');
  } catch (err) {
    console.error('❌ Error:', err);
  } finally {
    await connection.end(); // PENTING supaya child process close
    process.exit(0); // PENTING untuk loadfile.js
  }
}

run();
