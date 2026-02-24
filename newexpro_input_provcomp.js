const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const pool = require('./connection'); // pakai pool
const { insertDate } = require('./currentDate');

/* =========================================
   HAPUS DATA TANGGAL HARI INI
========================================= */
async function deleteExistingData() {
  const tables = ['provcomp', 'provcomp_wifi'];

  for (const table of tables) {
    const sql = `DELETE FROM ${table} WHERE tgl = ?`;
    await pool.execute(sql, [insertDate]);
    console.log(`✅ Data lama dihapus dari ${table}`);
  }
}

/* =========================================
   HAPUS BARIS TIDAK DIPERLUKAN
========================================= */
async function deleteUnwantedRows(table) {
  const values = ['TOTAL', 'REGIONAL', 'null'];

  for (const value of values) {
    const sql = `DELETE FROM ${table} WHERE regional = ?`;
    await pool.execute(sql, [value]);
  }
}

/* =========================================
   INPUT CSV KE DATABASE
========================================= */
async function inputDataToDatabase(file, insertToTable, jenis) {
  const filePath = path.join(__dirname, 'loaded_file/ff_non_hsi', `${file}.csv`);
  const insertPromises = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv({ separator: ',', headers: false }))

      .on('data', (data) => {
        const tgl = insertDate;
        const witel = data[Object.keys(data)[0]] || '';
        const real = data[Object.keys(data)[5]] || '';

        let newWitel = '';

        if (jenis === 'tif') {
          newWitel = witel.replace('REG-', 'TERRITORY 0');
        } else {
          newWitel = witel.replace('REG-', 'REGIONAL 0');
        }

        const lokasi = newWitel.replace('JATIM BARAT', 'MALANG').replace('JATIM TIMUR', 'SIDOARJO').replace('SEMARANG JATENG UTARA', 'SEMARANG').replace('SOLO JATENG TIMUR', 'SOLO').replace('YOGYA JATENG SELATAN', 'YOGYAKARTA');

        const sql = `
          INSERT INTO ${insertToTable}
          (tgl, jenis, regional, comply)
          VALUES (?, ?, ?, ?)
        `;

        insertPromises.push(pool.execute(sql, [tgl, jenis, lokasi, real]));
      })

      .on('end', async () => {
        try {
          await Promise.all(insertPromises);
          await deleteUnwantedRows(insertToTable);
          console.log(`✅ ${file} berhasil diinput ke tabel ${insertToTable}`);
          resolve();
        } catch (err) {
          reject(err);
        }
      })

      .on('error', reject);
  });
}

async function insert_data_ccm(table) {
  const sql = `
    INSERT INTO ${table} (tgl, jenis, regional, comply)

    SELECT
      tgl,
      'area_ccm',
      'BALI NUSRA',
      CASE
        WHEN ROUND(AVG(NULLIF(comply, '-')), 2) IS NULL
        THEN '-'
        ELSE ROUND(AVG(NULLIF(comply, '-')), 2)
      END
    FROM ${table}
    WHERE tgl = ?
      AND jenis = 'reg'
      AND regional IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
    GROUP BY tgl

    UNION ALL

    SELECT
      tgl,
      'area_ccm',
      'JAWA TIMUR',
      CASE
        WHEN ROUND(AVG(NULLIF(comply, '-')), 2) IS NULL
        THEN '-'
        ELSE ROUND(AVG(NULLIF(comply, '-')), 2)
      END
    FROM ${table}
    WHERE tgl = ?
      AND jenis = 'reg'
      AND regional IN
      ('MADIUN','MALANG','JEMBER','SIDOARJO','SURABAYA SELATAN',
       'SURABAYA UTARA','MADURA','PASURUAN')
    GROUP BY tgl

    UNION ALL

    SELECT
      tgl,
      'area_ccm',
      'JATENG DIY',
      CASE
        WHEN ROUND(AVG(NULLIF(comply, '-')), 2) IS NULL
        THEN '-'
        ELSE ROUND(AVG(NULLIF(comply, '-')), 2)
      END
    FROM ${table}
    WHERE tgl = ?
      AND jenis = 'reg'
      AND regional IN
      ('KUDUS','MAGELANG','PEKALONGAN','PURWOKERTO',
       'SEMARANG','SOLO','YOGYAKARTA')
    GROUP BY tgl
  `;

  await pool.execute(sql, [insertDate, insertDate, insertDate]);

  console.log(`✅ Insert area_ccm ke ${table} berhasil`);
}

/* =========================================
   MAIN RUNNER
========================================= */
async function run() {
  try {
    console.log('🔄 Menghapus data lama...');
    await deleteExistingData();

    const files = [
      ['provcomp_tif', 'provcomp', 'tif'],
      ['provcomp_district', 'provcomp', 'tif'],
      ['provcomp_reg', 'provcomp', 'reg'],
      ['provcomp_reg4', 'provcomp', 'reg'],
      ['provcomp_reg5', 'provcomp', 'reg'],

      ['provcomp_wifi_tif', 'provcomp_wifi', 'tif'],
      ['provcomp_wifi_district', 'provcomp_wifi', 'tif'],
      ['provcomp_wifi_reg', 'provcomp_wifi', 'reg'],
      ['provcomp_wifi_reg4', 'provcomp_wifi', 'reg'],
      ['provcomp_wifi_reg5', 'provcomp_wifi', 'reg'],
    ];

    for (const [file, table, jenis] of files) {
      await inputDataToDatabase(file, table, jenis);
    }
    await insert_data_ccm('provcomp');
    await insert_data_ccm('provcomp_wifi');

    console.log('🎉 SEMUA FILE BERHASIL DIPROSES');
  } catch (err) {
    console.error('❌ Error during processing:', err);
  } finally {
    await pool.end(); // tutup pool dengan benar
  }
}

run();
