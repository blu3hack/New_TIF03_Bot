const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const pool = require('./connection');
const { insertDate } = require('./currentDate');

// ================= DELETE DATA HARI INI =================
async function deleteExistingData() {
  const sql = `DELETE FROM ps_re WHERE tgl = ?`;
  await pool.execute(sql, [insertDate]);
}

// ================= DELETE ROW TIDAK DIPERLUKAN =================
async function deleteUnwantedRows() {
  const deleteValues = ['TOTAL', '', '(CANCEL+FALLOUT)', 'GROSS', 'NETT', 'Table 2', 'TERRITORY'];

  const placeholders = deleteValues.map(() => '?').join(',');

  const sql = `
    DELETE FROM ps_re
    WHERE tgl = ?
    AND lokasi IN (${placeholders})
  `;

  await pool.execute(sql, [insertDate, ...deleteValues]);
}

// ================= INPUT CSV =================
function inputDataToDatabase(file, area) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'loaded_file/ps_re', `${file}.csv`);

    const rowsToInsert = [];

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

        rowsToInsert.push([witel, area, psre, insertDate]);
      })
      .on('end', async () => {
        try {
          if (rowsToInsert.length > 0) {
            const sql = `
              INSERT INTO ps_re (lokasi, area, psre, tgl)
              VALUES ?
            `;

            await pool.query(sql, [rowsToInsert]); // bulk insert (lebih cepat & aman)
          }

          console.log(`${file} berhasil diinput ke ps_re`);
          resolve();
        } catch (err) {
          reject(err);
        }
      })
      .on('error', reject);
  });
}

// ================= HITUNG AREA CCM =================
async function insertAreaCCM() {
  const sql = `
    INSERT INTO ps_re (lokasi, area, psre, tgl)

    SELECT 'BALI NUSRA','area_ccm',
    ROUND(AVG(NULLIF(psre, '-')), 2), tgl
    FROM ps_re
    WHERE tgl = ? AND area = 'reg'
    AND lokasi IN ('DENPASAR','SINGARAJA','NTB','NTT')
    GROUP BY tgl

    UNION ALL

    SELECT 'JAWA TIMUR','area_ccm',
    ROUND(AVG(NULLIF(psre, '-')), 2), tgl
    FROM ps_re
    WHERE tgl = ? AND area = 'reg'
    AND lokasi IN (
      'MADIUN','MALANG','JEMBER','SIDOARJO',
      'SURABAYA SELATAN','SURABAYA UTARA',
      'MADURA','PASURUAN'
    )
    GROUP BY tgl

    UNION ALL

    SELECT 'JATENG DIY','area_ccm',
    ROUND(AVG(NULLIF(psre, '-')), 2), tgl
    FROM ps_re
    WHERE tgl = ? AND area = 'reg'
    AND lokasi IN (
      'KUDUS','MAGELANG','PEKALONGAN',
      'PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA'
    )
    GROUP BY tgl
  `;

  await pool.execute(sql, [insertDate, insertDate, insertDate]);

  console.log('Insert area_ccm berhasil');
}

// ================= MAIN =================
async function run() {
  try {
    await deleteExistingData();

    await inputDataToDatabase('ps_re_tif', 'tif');
    await inputDataToDatabase('ps_re_reg', 'reg');

    await deleteUnwantedRows();

    await insertAreaCCM();

    console.log('SEMUA FILE BERHASIL DIPROSES ✅');
  } catch (err) {
    console.error('Error during data processing:', err);
  } finally {
    await pool.end(); // tutup pool dengan benar
  }
}

run();
