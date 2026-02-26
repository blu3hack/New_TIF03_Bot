const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const connection = require('./connection');
const { insertDate } = require('./currentDate');
const path = require('path');

// ================= DELETE DATA =================
async function deleteExistingData() {
  const tables = ['ttr_datin', 'ttr_reseller', 'ttr_indibiz', 'ttr_siptrunk', 'ttr_dwdm', 'sugar_datin', 'hsi_sugar'];

  for (const table of tables) {
    const sql = `DELETE FROM ${table} WHERE tgl = ?`;
    await connection.execute(sql, [insertDate]);
  }
}

// ================= INPUT CSV =================
async function inputDataToDatabase(file, jenis, insertToTable) {
  const filePath = path.join(__dirname, 'loaded_file/asr_datin', `${file}.csv`);
  const rows = [];

  await new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => {
        const keys = Object.keys(data);
        const witel = data[keys[0]] || '';

        const k1 = (data[keys[6]] || '').replace(/%/g, '');
        const k2 = (data[keys[13]] || '').replace(/%/g, '');
        const k3 = (data[keys[20]] || '').replace(/%/g, '');

        const sugar = (data[keys[5]] || '').replace(/%/g, '');

        const resel_indi_6h_4h = (data[keys[4]] || '').replace(/%/g, '');
        const resel_indi_36h_24h = (data[keys[9]] || '').replace(/%/g, '');

        const target_siptrunk_dwdm = (data[keys[1]] || '').replace(/%/g, '');
        const real_siptrunk_dwdm = (data[keys[3]] || '').replace(/%/g, '');
        const ach_siptrunk_dwdm = (data[keys[4]] || '').replace(/%/g, '');

        const fixwitel = witel.replace('REG-', 'REGIONAL 0').replace('TERRITORY ', 'TERRITORY 0');

        rows.push({
          jenis,
          fixwitel,
          tgl: insertDate,
          k1,
          k2,
          k3,
          sugar,
          resel_indi_6h_4h,
          resel_indi_36h_24h,
          target_siptrunk_dwdm,
          real_siptrunk_dwdm,
          ach_siptrunk_dwdm,
        });
      })
      .on('end', resolve)
      .on('error', reject);
  });

  // 🔥 Insert ke DB
  for (const row of rows) {
    if (insertToTable === 'ttr_datin') {
      await connection.execute(
        `INSERT INTO ${insertToTable} (jenis, treg, tgl, k1, k2, k3)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [row.jenis, row.fixwitel, row.tgl, row.k1, row.k2, row.k3],
      );
    } else if (insertToTable === 'ttr_reseller' || insertToTable === 'ttr_indibiz') {
      await connection.execute(
        `INSERT INTO ${insertToTable} (jenis, treg, tgl, real_1, real_2)
         VALUES (?, ?, ?, ?, ?)`,
        [row.jenis, row.fixwitel, row.tgl, row.resel_indi_6h_4h, row.resel_indi_36h_24h],
      );
    } else if (insertToTable === 'ttr_siptrunk' || insertToTable === 'ttr_dwdm') {
      await connection.execute(
        `INSERT INTO ${insertToTable} (jenis, treg, tgl, target, \`real\`, ach)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [row.jenis, row.fixwitel, row.tgl, row.target_siptrunk_dwdm, row.real_siptrunk_dwdm, row.ach_siptrunk_dwdm],
      );
    } else {
      await connection.execute(`INSERT INTO ${insertToTable} (jenis, treg, tgl, \`real\`) VALUES (?, ?, ?,?)`, [row.jenis, row.fixwitel, row.tgl, row.sugar]);
    }
  }

  await deleteUnwantedRows(insertToTable);
  console.log(`✅ ${file} berhasil diinput ke tabel ${insertToTable}`);
}

// ================= DELETE DUPLICATE =================
async function delete_ttr_datin_duplicate() {
  const table_for_delete = ['ttr_datin', 'ttr_reseller', 'ttr_indibiz', 'ttr_siptrunk', 'ttr_dwdm', 'sugar_datin', 'hsi_sugar'];
  for (const table of table_for_delete) {
    const query = `delete from ${table} where tgl = ? and treg NOT LIKE '%TERRITORY%'`;
    await connection.execute(query, [insertDate]);
  }
}

// ================= DELETE UNWANTED =================
async function deleteUnwantedRows() {
  const table_for_delete = ['ttr_datin', 'ttr_reseller', 'ttr_indibiz', 'ttr_siptrunk', 'ttr_dwdm'];
  for (const table of table_for_delete) {
    const query = `delete from ${table} where tgl = ? and treg = 'TERRITORY 0NAS'`;
    await connection.execute(query, [insertDate]);
  }
}

// ================= MAIN =================
async function main() {
  try {
    await deleteExistingData();

    const inputList = [
      ['ttr_datin_tif', 'tif', 'ttr_datin'],
      ['indibiz_tif', 'tif', 'ttr_indibiz'],
      ['reseller_tif', 'tif', 'ttr_reseller'],
      ['siptrunk_tif', 'tif', 'ttr_siptrunk'],
      ['dwdm_tif', 'tif', 'ttr_dwdm'],
      ['sugar_datin_tif', 'tif', 'sugar_datin'],
      ['sugar_hsi_tif', 'tif', 'hsi_sugar'],
    ];

    for (const [file, jenis, table] of inputList) {
      await inputDataToDatabase(file, jenis, table);
    }

    await delete_ttr_datin_duplicate();
    await deleteUnwantedRows();
  } catch (err) {
    console.error('❌ Error di main():', err);
  } finally {
    await connection.end();
    console.log('🔒 Koneksi database ditutup.');
  }
}

main();
