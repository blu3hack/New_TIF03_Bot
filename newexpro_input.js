const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const connection = require('./connection');
const { insertDate } = require('./currentDate');

// Helper untuk query Promise
function queryAsync(sql, params = []) {
  return new Promise((resolve, reject) => {
    connection.query(sql, params, (err, results) => {
      if (err) return reject(err);
      resolve(results);
    });
  });
}

// ✅ Hapus data berdasarkan tanggal hari ini (menunggu selesai semua)
async function deleteExistingData() {
  const tableForDelete = ['ffg_non_hsi', 'ttr_ffg_non_hsi', 'ttd_non_hsi', 'ttd_wifi'];

  const currentDate = insertDate;

  for (const table of tableForDelete) {
    const sql = `DELETE FROM ${table} WHERE tgl = ?`;
    await queryAsync(sql, [currentDate]);
    console.log(`Data lama dihapus dari ${table}`);
  }
}

// ✅ Hapus baris tidak diperlukan
async function deleteUnwantedRows(table) {
  const deleteWitelValues = ['TOTAL', 'REGIONAL', 'null'];

  for (const value of deleteWitelValues) {
    const sql = `DELETE FROM ${table} WHERE regional = ?`;
    await queryAsync(sql, [value]);
  }
}

// ✅ Input CSV ke database (benar-benar menunggu selesai)
function inputDataToDatabase(file, insertToTable, jenis) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'loaded_file/ff_non_hsi', `${file}.csv`);

    const insertPromises = [];

    fs.createReadStream(filePath)
      .pipe(csv({ separator: ',', headers: false }))
      .on('data', (data) => {
        const tgl = insertDate;
        const witel = data[Object.keys(data)[0]] || '';

        let real = '';

        if (insertToTable === 'ttd_non_hsi' || insertToTable === 'ttd_wifi') {
          real = data[Object.keys(data)[4]] || '';
        } else {
          let avg = data[Object.keys(data)[1]] || '';
          let jml = data[Object.keys(data)[2]] || '';

          if (avg == 0 || jml == 0) {
            real = 100;
          } else {
            real = data[Object.keys(data)[3]] || '';
          }
        }

        let newWitel = '';

        if (jenis === 'tif') {
          newWitel = witel.replace('REG-', 'TERRITORY 0');
        } else {
          newWitel = witel.replace('REG-', 'REGIONAL 0');
        }

        const lokasi = newWitel.replace('JATIM BARAT', 'MALANG').replace('JATIM TIMUR', 'SIDOARJO').replace('SEMARANG JATENG UTARA', 'SEMARANG').replace('SOLO JATENG TIMUR', 'SOLO').replace('YOGYA JATENG SELATAN', 'YOGYAKARTA');

        const query = `
          INSERT INTO ${insertToTable}
          (tgl, jenis, regional, comply)
          VALUES (?, ?, ?, ?)
        `;

        insertPromises.push(queryAsync(query, [tgl, jenis, lokasi, real]));
      })
      .on('end', async () => {
        try {
          await Promise.all(insertPromises);
          await deleteUnwantedRows(insertToTable);

          console.log(`${file} berhasil diinput ke tabel ${insertToTable}`);
          resolve();
        } catch (err) {
          reject(err);
        }
      })
      .on('error', reject);
  });
}

async function insert_data_ccm(table) {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO ${table} (tgl, jenis, regional, comply)
    SELECT
      tgl,
      'area_ccm' AS jenis,
      'BALI NUSRA' AS regional,

      CASE WHEN ROUND(AVG(NULLIF(comply, '-')), 2) IS NULL THEN '-' ELSE ROUND(AVG(NULLIF(comply, '-')), 2) END AS comply


    FROM ${table}
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND regional IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
    GROUP BY tgl

    UNION ALL

    SELECT
    tgl,
      'area_ccm' AS jenis,
      'JAWA TIMUR' AS regional,
      CASE WHEN ROUND(AVG(NULLIF(comply, '-')), 2) IS NULL THEN '-' ELSE ROUND(AVG(NULLIF(comply, '-')), 2) END AS comply
    FROM ${table}
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND regional IN ('MADIUN','MALANG','JEMBER','SIDOARJO','SURABAYA SELATAN','SURABAYA UTARA','MADURA','PASURUAN')
    GROUP BY tgl

    UNION ALL

    SELECT
    tgl,
      'area_ccm' AS jenis,
      'JATENG DIY' AS regional,
      CASE WHEN ROUND(AVG(NULLIF(comply, '-')), 2) IS NULL THEN '-' ELSE ROUND(AVG(NULLIF(comply, '-')), 2) END AS comply
    FROM ${table}
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND regional IN ('KUDUS','MAGELANG','PEKALONGAN','PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA')
    GROUP BY tgl
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log(`Insert ke table ${table} berhasil:`);
  } catch (err) {
    console.error('Error insert:', err);
  }
}

// ✅ Main Runner
async function run() {
  try {
    await deleteExistingData();
    const files = [
      ['ffg_tif', 'ffg_non_hsi', 'tif'],
      ['ffg_district', 'ffg_non_hsi', 'tif'],
      ['ffg_reg', 'ffg_non_hsi', 'reg'],
      ['ffg_reg4', 'ffg_non_hsi', 'reg'],
      ['ffg_reg5', 'ffg_non_hsi', 'reg'],

      ['ttr_ffg_tif', 'ttr_ffg_non_hsi', 'tif'],
      ['ttr_ffg_district', 'ttr_ffg_non_hsi', 'tif'],
      ['ttr_ffg_reg', 'ttr_ffg_non_hsi', 'reg'],
      ['ttr_ffg_reg4', 'ttr_ffg_non_hsi', 'reg'],
      ['ttr_ffg_reg5', 'ttr_ffg_non_hsi', 'reg'],

      ['ttdc_tif', 'ttd_non_hsi', 'tif'],
      ['ttdc_district', 'ttd_non_hsi', 'tif'],
      ['ttdc_reg', 'ttd_non_hsi', 'reg'],
      ['ttdc_reg4', 'ttd_non_hsi', 'reg'],
      ['ttdc_reg5', 'ttd_non_hsi', 'reg'],

      ['ttdc_wifi_tif', 'ttd_wifi', 'tif'],
      ['ttdc_wifi_district', 'ttd_wifi', 'tif'],
      ['ttdc_wifi_reg', 'ttd_wifi', 'reg'],
      ['ttdc_wifi_reg4', 'ttd_wifi', 'reg'],
      ['ttdc_wifi_reg5', 'ttd_wifi', 'reg'],
    ];

    // ✅ Urutan parameter sudah benar
    for (const [file, insertToTable, jenis] of files) {
      await inputDataToDatabase(file, insertToTable, jenis);
    }

    await insert_data_ccm('ttd_non_hsi');
    await insert_data_ccm('ttd_wifi');
    await insert_data_ccm('ttr_ffg_non_hsi');
    await insert_data_ccm('ffg_non_hsi');

    console.log('SEMUA FILE BERHASIL DIPROSES ✅');
  } catch (err) {
    console.error('Error during data processing:', err);
  } finally {
    connection.end();
  }
}
run();
