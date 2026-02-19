const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const connection = require('./connection');
const { insertDate } = require('./currentDate');

// Helper promise query
function queryAsync(sql, params = []) {
  return new Promise((resolve, reject) => {
    connection.query(sql, params, (err, results) => {
      if (err) return reject(err);
      resolve(results);
    });
  });
}

// ✅ Hapus data dengan tanggal hari ini (benar-benar menunggu selesai)
async function deleteExistingData() {
  const currentDate = insertDate;
  await queryAsync(`DELETE FROM ps_re WHERE tgl = ?`, [currentDate]);
  console.log('Data lama berhasil dihapus');
}

// ✅ Hapus baris TOTAL atau kosong (menunggu selesai)
async function deleteUnwantedRows(table) {
  const deleteWitelValues = ['TOTAL', '', '(CANCEL+FALLOUT)', 'GROSS', 'NETT', 'Table 2', 'TERRITORY'];

  for (const value of deleteWitelValues) {
    await queryAsync(`DELETE FROM ${table} WHERE lokasi = ?`, [value]);
  }
}

// ✅ Baca CSV dan insert ke database (menunggu semua insert selesai)
function inputDataToDatabase(file, area, insertToTable) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'loaded_file/ps_re', `${file}.csv`);
    const currentDate = insertDate;

    const insertPromises = [];

    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => {
        let witel = data[Object.keys(data)[0]] || '';

        const witelMappings = [
          { from: /^MATARAM\s*/i, to: 'NTB ' },
          { from: /^KUPANG\s*/i, to: 'NTT ' },
          { from: /^SINGARJA/i, to: 'SINGARAJA' },
          { from: /^SBY UTARA/i, to: 'SURABAYA UTARA' },
          { from: /^SBY SELATAN/i, to: 'SURABAYA SELATAN' },
        ];

        for (const map of witelMappings) {
          if (map.from.test(witel)) {
            witel = witel.replace(map.from, map.to);
            break;
          }
        }

        if (area === 'reg') {
          witel = witel.replace('TERRITORY ', 'REGIONAL 0');
        } else {
          witel = witel.replace('TERRITORY ', 'TERRITORY 0');
        }

        const psre = data[Object.keys(data)[6]] || '';

        const query = `
          INSERT INTO ${insertToTable}
          (lokasi, area, psre, tgl)
          VALUES (?, ?, ?, ?)
        `;

        insertPromises.push(queryAsync(query, [witel, area, psre, currentDate]));
      })
      .on('end', async () => {
        try {
          await Promise.all(insertPromises); // tunggu semua insert
          await deleteUnwantedRows(insertToTable); // tunggu delete
          console.log(`${file} berhasil diinput ke tabel ${insertToTable}`);
          resolve();
        } catch (err) {
          reject(err);
        }
      })
      .on('error', reject);
  });
}

async function ps_re() {
  const currentDate = insertDate;

  const sql = `
    INSERT INTO ps_re (lokasi, area, psre, tgl)

    SELECT
      'BALI NUSRA',
      'area_ccm',
      ROUND(AVG(NULLIF(psre, '-')), 2),
      tgl
    FROM ps_re
    WHERE tgl = ?
      AND area = 'reg'
      AND lokasi IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
    GROUP BY tgl

    UNION ALL

    SELECT
      'JAWA TIMUR',
      'area_ccm',
      ROUND(AVG(NULLIF(psre, '-')), 2),
      tgl
    FROM ps_re
    WHERE tgl = ?
      AND area = 'reg'
      AND lokasi IN (
        'MADIUN','MALANG','JEMBER','SIDOARJO',
        'SURABAYA SELATAN','SURABAYA UTARA',
        'MADURA','PASURUAN'
      )
    GROUP BY tgl

    UNION ALL

    SELECT
      'JATENG DIY',
      'area_ccm',
      ROUND(AVG(NULLIF(psre, '-')), 2),
      tgl
    FROM ps_re
    WHERE tgl = ?
      AND area = 'reg'
      AND lokasi IN (
        'KUDUS','MAGELANG','PEKALONGAN',
        'PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA'
      )
    GROUP BY tgl
  `;

  try {
    await queryAsync(sql, [currentDate, currentDate, currentDate]);
    console.log('Insert ke table ps re berhasil:');
  } catch (err) {
    console.error('Error insert ps re:', err);
  }
}

// ✅ MAIN
async function run() {
  try {
    await deleteExistingData();

    await inputDataToDatabase('ps_re_tif', 'tif', 'ps_re');
    await inputDataToDatabase('ps_re_reg', 'reg', 'ps_re');
    await ps_re(); // tunggu proses ps_re selesai

    console.log('SEMUA FILE BERHASIL DIPROSES ✅');
  } catch (err) {
    console.error('Error during data processing:', err);
  } finally {
    connection.end(); // sekarang aman karena semua sudah selesai
  }
}

run();
