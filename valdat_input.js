const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const pool = require('./connection');
const { insertDate } = require('./currentDate');

// ================= DELETE DATA HARI INI =================
async function deleteExistingData() {
  const sql = `DELETE FROM valdat_new WHERE tgl = ?`;
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
function inputDataToDatabase(file, jenis) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'loaded_file/valdat', `${file}.csv`);
    const rowsToInsert = [];
    const insertDate = new Date().toISOString().split('T')[0];

    const cleanNumber = (val) => {
      if (!val) return 0;
      return parseFloat(val.toString().replace('%', '').trim()) || 0;
    };

    fs.createReadStream(filePath)
      .pipe(csv({ headers: false }))
      .on('data', (data) => {
        // Abaikan baris kosong
        if (!data[0] && !data[1]) return;

        // 1. Deklarasi variabel penampung (Scope Luar)
        let witel = '';
        let valdat = 0;
        let target = 0;
        let ach = 0;

        // 2. Logika berdasarkan jenis
        if (jenis === 'tif') {
          const witelRaw = data[1] || '';
          if (witelRaw.includes('SUMATERA')) witel = 'TERRITORY 01';
          else if (witelRaw.includes('JABODETABEK')) witel = 'TERRITORY 02';
          else if (witelRaw.includes('JAWA BALI')) witel = 'TERRITORY 03';
          else if (witelRaw.includes('PAMASUKA')) witel = 'TERRITORY 04';
          else witel = witelRaw;

          valdat = cleanNumber(data[32]);
          target = cleanNumber(data[33]);
          ach = cleanNumber(data[34]);
        } else if (jenis === 'area_ccm') {
          const witelRaw = data[2] || ''; // Witel/Area di Index 2

          if (witelRaw.includes('JATIM')) witel = 'JAWA TIMUR';
          else if (witelRaw.includes('BALINUSRA')) witel = 'BALI NUSRA';
          else if (witelRaw.includes('JATENG DIY')) witel = 'JATENG DIY';
          else witel = witelRaw;

          valdat = cleanNumber(data[33]);
          target = cleanNumber(data[34]);
          ach = cleanNumber(data[35]);
        } else {
          // Logika Default (Asumsi untuk jenis lain)
          witel = data[3] || data[2] || 'UNKNOWN';
          valdat = cleanNumber(data[33]);
          target = cleanNumber(data[34]);
          ach = cleanNumber(data[35]);
        }

        // 3. Push ke array jika baris valid
        if (witel) {
          rowsToInsert.push([insertDate, witel, jenis, valdat, target, ach]);
        }
      })
      .on('end', async () => {
        try {
          if (rowsToInsert.length > 0) {
            const sql = `INSERT INTO valdat_new (tgl, witel, jenis, realisasi, tgt, ach) VALUES ?`;
            await pool.query(sql, [rowsToInsert]);
            console.log(`✅ ${file} berhasil diinput.`);
          }
          resolve();
        } catch (err) {
          console.error('❌ Gagal Insert:', err.message);
          reject(err);
        }
      })
      .on('error', reject);
  });
}
// ================= MAIN =================
async function run() {
  try {
    await deleteExistingData();
    await inputDataToDatabase('area', 'tif');
    await inputDataToDatabase('region', 'area_ccm');
    await inputDataToDatabase('bali', 'balnus_ccm');
    await inputDataToDatabase('jatim', 'jatim_ccm');
    await inputDataToDatabase('jateng', 'jateng_ccm');
    // await deleteUnwantedRows();
    console.log('SEMUA FILE BERHASIL DIPROSES ✅');
  } catch (err) {
    console.error('Error during data processing:', err);
  } finally {
    await pool.end(); // tutup pool dengan benar
  }
}

run();
