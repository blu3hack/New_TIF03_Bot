const fs = require('fs');
const path = require('path');
const pool = require('./connection'); // Menggunakan pool dari mysql2/promise
const { insertDate } = require('./currentDate');

// 1. Fungsi Input CSV menggunakan LOAD DATA LOCAL INFILE
async function wifi_revi_reg(fileName, jenis) {
  const filePath = path.join(__dirname, 'loaded_file/wifi_revi', `${fileName}.csv`).replace(/\\/g, '/');

  const query = `
    LOAD DATA LOCAL INFILE ?
    INTO TABLE wifi_revi_reg
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    (A,lokasi,C,D,E,F,G,H,I,J,K,realisasi,M,N,O,P,Q,R,S,T,U)
    SET jenis = ?`;

  try {
    // Note: LOAD DATA LOCAL memerlukan konfigurasi khusus pada pool/connection
    await pool.query({
      sql: query,
      values: [filePath, jenis],
      infileStreamFactory: (path) => fs.createReadStream(path),
    });
    console.log(`✅ ${fileName}.csv berhasil diinput ke database`);
  } catch (err) {
    console.error(`❌ Gagal input ${fileName}.csv:`, err.message);
    throw err;
  }
}

// 2. Membersihkan data berdasarkan list lokasi
async function deleteDataByCondition() {
  const searchStrings = [
    'TREG 1',
    'TREG 2',
    'TREG 3',
    'TREG 4',
    'TREG 5',
    'TREG 6',
    'TREG 7',
    'KUDUS',
    'MAGELANG',
    'PEKALONGAN',
    'PURWOKERTO',
    'SEMARANG',
    'SOLO',
    'YOGYAKARTA',
    'DENPASAR',
    'JEMBER',
    'KEDIRI',
    'MADIUN',
    'MADURA',
    'MALANG',
    'NTB',
    'NTT',
    'PASURUAN',
    'SIDOARJO',
    'SINGARAJA',
    'SURABAYA SELATAN',
    'SURABAYA UTARA',
    'BALI',
    'JATIM BARAT',
    'JATIM TIMUR',
    'NUSA TENGGARA',
    'SEMARANG JATENG UTARA',
    'SOLO JATENG TIMUR',
    'SURAMADU',
    'YOGYA JATENG SELATAN',
  ];

  // Menggunakan NOT LIKE untuk setiap string
  const whereClause = searchStrings.map(() => 'lokasi NOT LIKE ?').join(' AND ');
  const sql = `DELETE FROM wifi_revi_reg WHERE ${whereClause}`;

  try {
    const [results] = await pool.query(
      sql,
      searchStrings.map((s) => `%${s}%`),
    );
    console.log(`🗑️ Deleted ${results.affectedRows} rows from wifi_revi_reg`);
  } catch (err) {
    console.error('❌ Error deleteDataByCondition:', err.message);
    throw err;
  }
}

// 3. Memindahkan data ke tabel final (wifi_revi)
async function insert_data() {
  const currentDate = insertDate;

  try {
    // Hapus data hari ini di tabel tujuan
    const [delRes] = await pool.query('DELETE FROM wifi_revi WHERE tgl = ?', [currentDate]);
    console.log(`🧹 Berhasil menghapus ${delRes.affectedRows} baris lama di wifi_revi.`);

    // Ambil data dari tabel temporary
    const [rows] = await pool.query('SELECT lokasi, K, realisasi, jenis FROM wifi_revi_reg');

    if (rows.length === 0) {
      console.log('⚠️ Tidak ada data untuk dipindahkan.');
      return;
    }

    const lokasiMapping = {
      'JATIM BARAT': 'MALANG',
      'JATIM TIMUR': 'SIDOARJO',
      'SEMARANG JATENG UTARA': 'SEMARANG',
      'SOLO JATENG TIMUR': 'SOLO',
      'YOGYA JATENG SELATAN': 'YOGYAKARTA',
    };

    const values = rows.map((row) => {
      let lokasi = row.lokasi.replace('TREG ', row.jenis === 'tif' ? 'TERRITORY 0' : 'REGIONAL 0');
      lokasi = lokasiMapping[lokasi] || lokasi;

      let realisasi = row.realisasi;
      if (row.K == 0 && row.realisasi == 0) {
        realisasi = '-';
      }
      return [currentDate, row.jenis, lokasi, realisasi];
    });

    const insertQuery = 'INSERT INTO wifi_revi (tgl, jenis, regional, comply) VALUES ?';
    const [insRes] = await pool.query(insertQuery, [values]);
    console.log(`✅ Berhasil memasukkan ${insRes.affectedRows} baris ke wifi_revi.`);
  } catch (err) {
    console.error('❌ Error insert_data:', err.message);
    throw err;
  }
}

// 4. Reset tabel temporary
async function deleteAllData() {
  try {
    const [result] = await pool.query('DELETE FROM wifi_revi_reg');
    console.log(`🗑️ Reset temporary table wifi_revi_reg (${result.affectedRows} rows).`);
  } catch (err) {
    console.error('❌ Error deleteAllData:', err.message);
    throw err;
  }
}

// 5. Hapus duplikat khusus lokasi MALANG
async function deleteDuplicates(jenis) {
  const currentDate = insertDate;
  const sql = `
      DELETE FROM wifi_revi
      WHERE id NOT IN (
        SELECT * FROM (
          SELECT MIN(id) AS keep_id
          FROM wifi_revi
          WHERE tgl = ?
            AND jenis = ?
            AND regional = 'MALANG'
            AND comply = '-'
          GROUP BY regional, comply
        ) AS keep_ids
      )
      AND tgl = ?
      AND jenis = ?
      AND regional = 'MALANG'
      AND comply = '-'
    `;

  try {
    const [result] = await pool.query(sql, [currentDate, jenis, currentDate, jenis]);
    console.log(`✅ Duplikat MALANG (${jenis}) dibersihkan: ${result.affectedRows} baris.`);
  } catch (err) {
    console.error(`❌ Error deleteDuplicates ${jenis}:`, err.message);
    throw err;
  }
}

async function insert_ccm_group1(table) {
  const currentDate = insertDate;

  // Query tetap sama seperti asli, menggunakan UNION ALL untuk 3 area
  const sql = `
    INSERT INTO ${table} (tgl, jenis, regional, comply)
    SELECT
      tgl,
      'area_ccm' AS jenis,
      'BALI NUSRA' AS regional,
      ROUND(AVG(NULLIF(comply, '-')), 2) AS comply
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
      ROUND(AVG(NULLIF(comply, '-')), 2) AS comply
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
      ROUND(AVG(NULLIF(comply, '-')), 2) AS comply
    FROM ${table}
    WHERE
      tgl = ?
      AND jenis = 'reg'
      AND regional IN ('KUDUS','MAGELANG','PEKALONGAN','PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA')
    GROUP BY tgl
  `;

  try {
    // Menggunakan pool.query dengan passing currentDate 3 kali sesuai jumlah placeholder (?)
    await pool.query(sql, [currentDate, currentDate, currentDate]);
    console.log(`✅ Insert ke table ${table} (Area CCM) berhasil.`);
  } catch (err) {
    console.error(`❌ Error insert CCM Group pada table ${table}:`, err.message);
    // Kita tidak menggunakan throw agar proses di main() tetap berlanjut ke table berikutnya
  }
}

// 6. JALANKAN PROSES
async function run() {
  try {
    console.log('🔄 Memulai proses ETL Wifi Revi...');

    await wifi_revi_reg('wifi_revi_reg', 'reg');
    await wifi_revi_reg('wifi_revi_tif', 'tif');
    await deleteDataByCondition();
    await insert_data();
    await deleteAllData();
    await deleteDuplicates('tif');
    await deleteDuplicates('reg');
    await insert_ccm_group1('wifi_revi');

    console.log('🏁 Seluruh proses Wifi Revi selesai!');
  } catch (err) {
    console.error('💥 Terjadi kesalahan fatal:', err.message);
  } finally {
    pool.end();
  }
}

run();
