const mysql = require('mysql2/promise');
const { insertDate } = require('../currentDate');

async function insertPivotDataBothAreas(table, tanggal, lokasi, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const connection = await mysql.createConnection({
      host: 'xxx.xxx.xxx.xxx',
      user: 'xxxxxxxxx',
      password: 'xxxxxxxxxx',
      database: 'xxxxxxxxx',
    });

    try {
      await connection.beginTransaction();

      // Insert org_1
      await connection.execute(
        `
        INSERT INTO ttr_ffg_download (area, lokasi, comp, not_comp, total, realisasi, tgl)
        SELECT
        CASE
            WHEN org_1 LIKE 'TERRITORY %' THEN 
                CONCAT('TERRITORY ', LPAD(CAST(SUBSTRING_INDEX(org_1, ' ', -1) AS UNSIGNED), 2, '0'))
            WHEN org_1 LIKE 'TREG %' THEN 
                CONCAT('REGIONAL ', LPAD(CAST(SUBSTRING_INDEX(org_1, ' ', -1) AS UNSIGNED), 2, '0'))
            ELSE org_1
        END AS area, 
        ?, 
          COUNT(CASE WHEN f_ttr='TTR-COMP' THEN 1 END),
          COUNT(CASE WHEN f_ttr='TTR-NOTC' THEN 1 END),
          COUNT(CASE WHEN f_ttr='TTR-COMP' THEN 1 END)+COUNT(CASE WHEN f_ttr='TTR-NOTC' THEN 1 END),
          ROUND(COUNT(CASE WHEN f_ttr='TTR-COMP' THEN 1 END)/(COUNT(CASE WHEN f_ttr='TTR-COMP' THEN 1 END)+COUNT(CASE WHEN f_ttr='TTR-NOTC' THEN 1 END))*100,2),
          ? 
        FROM ${table} WHERE first_cust_assign IS NOT NULL
        GROUP BY org_1
        `,
        [lokasi, tanggal],
      );

      // Insert org_2
      await connection.execute(
        `
        INSERT INTO ttr_ffg_download (area, lokasi, comp, not_comp, total, realisasi, tgl)
        SELECT org_2 AS area, ?, 
          COUNT(CASE WHEN f_ttr='TTR-COMP' THEN 1 END),
          COUNT(CASE WHEN f_ttr='TTR-NOTC' THEN 1 END),
          COUNT(CASE WHEN f_ttr='TTR-COMP' THEN 1 END)+COUNT(CASE WHEN f_ttr='TTR-NOTC' THEN 1 END),
          ROUND(COUNT(CASE WHEN f_ttr='TTR-COMP' THEN 1 END)/(COUNT(CASE WHEN f_ttr='TTR-COMP' THEN 1 END)+COUNT(CASE WHEN f_ttr='TTR-NOTC' THEN 1 END))*100,2),
          ? 
        FROM ${table} WHERE first_cust_assign IS NOT NULL
        GROUP BY org_2
        `,
        [lokasi, tanggal],
      );

      await connection.commit();
      console.log(`Data dari ${table} untuk lokasi ${lokasi} berhasil dimasukkan.`);
      await connection.end();
      break; // keluar loop kalau berhasil
    } catch (err) {
      await connection.rollback();
      await connection.end();

      if (err.message.includes('Deadlock') && attempt < maxRetries) {
        console.warn(`Deadlock terdeteksi, mencoba ulang (${attempt}/${maxRetries})...`);
      } else {
        console.error('Terjadi kesalahan:', err.message);
        break;
      }
    }
  }
}

// Fungsi utama untuk menjalankan semua
async function runAll() {
  const connection = await mysql.createConnection({
    host: 'xxxxxxxxxxxxxxxx',
    user: 'xxxxxxxxxxx',
    password: '############',
    database: 'perf_tif',
  });

  try {
    // DELETE hanya sekali
    const [deleteResult] = await connection.execute(`DELETE FROM ttr_ffg_download WHERE tgl = ?`, [insertDate]);
    console.log(`Data lama dihapus: ${deleteResult.affectedRows} baris`);

    await connection.end();

    // Insert kedua tabel secara berurutan
    await insertPivotDataBothAreas('download_fulfillment_tif', insertDate, 'tif');
    await insertPivotDataBothAreas('download_fulfillment_regional', insertDate, 'reg');
  } catch (err) {
    console.error('Terjadi kesalahan saat delete:', err.message);
  }
}

runAll();
