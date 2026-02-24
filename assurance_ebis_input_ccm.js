const pool = require('./connection');
const { insertDate } = require('./currentDate');

// Fungsi hapus data berdasarkan tanggal hari ini
async function deleteExistingData() {
  const tableForDelete = ['unspec_datin', 'unspec_hsi', 'q_datin', 'q_hsi', 'sqm_datin', 'sqm_hsi'];

  const currentDate = insertDate;
  const jenis_for_delete = ['area_ccm'];

  console.log('🗑️ Membersihkan data area_ccm lama...');

  for (const table of tableForDelete) {
    let colomnName = 'jenis';
    let tglName = 'tgl';

    if (table === 'ttr_ffg_download' || table === 'cnop_latency') {
      colomnName = 'lokasi';
    } else if (table === 'ps_re') {
      colomnName = 'area';
    }

    if (table === 'cnop_latency') {
      tglName = 'insert_at';
    }

    const sql = `
      DELETE FROM ${table}
      WHERE ${tglName} = ?
      AND ${colomnName} IN (?)
    `;

    try {
      // 🔹 GUNAKAN pool.query (bukan execute) untuk operator IN dengan array
      await pool.query(sql, [currentDate, jenis_for_delete]);
    } catch (err) {
      console.error(`❌ Gagal hapus di tabel ${table}:`, err.message);
    }
  }
}

async function sqm_datin() {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO sqm_datin (tgl, jenis, regional, comply)
    SELECT tgl, 'area_ccm', 'BALI NUSRA', ROUND(AVG(NULLIF(comply, '-')), 2) FROM sqm_datin
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT') GROUP BY tgl
    UNION ALL
    SELECT tgl, 'area_ccm', 'JAWA TIMUR', ROUND(AVG(NULLIF(comply, '-')), 2) FROM sqm_datin
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('MADIUN','MALANG','JEMBER','SIDOARJO','SURABAYA SELATAN','SURABAYA UTARA','MADURA','PASURUAN') GROUP BY tgl
    UNION ALL
    SELECT tgl, 'area_ccm', 'JATENG DIY', ROUND(AVG(NULLIF(comply, '-')), 2) FROM sqm_datin
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('KUDUS','MAGELANG','PEKALONGAN','PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA') GROUP BY tgl
  `;

  try {
    await pool.query(sql, [currentDate, currentDate, currentDate]);
    console.log('✅ Insert ke table sqm_datin berhasil');
  } catch (err) {
    console.error('❌ Error insert sqm_datin:', err.message);
  }
}

async function sqm_hsi() {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO sqm_hsi (tgl, jenis, regional, comply)
    SELECT tgl, 'area_ccm', 'BALI NUSRA', ROUND(AVG(NULLIF(comply, '-')), 2) FROM sqm_hsi
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT') GROUP BY tgl
    UNION ALL
    SELECT tgl, 'area_ccm', 'JAWA TIMUR', ROUND(AVG(NULLIF(comply, '-')), 2) FROM sqm_hsi
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('MADIUN','MALANG','JEMBER','SIDOARJO','SURABAYA SELATAN','SURABAYA UTARA','MADURA','PASURUAN') GROUP BY tgl
    UNION ALL
    SELECT tgl, 'area_ccm', 'JATENG DIY', ROUND(AVG(NULLIF(comply, '-')), 2) FROM sqm_hsi
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('KUDUS','MAGELANG','PEKALONGAN','PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA') GROUP BY tgl
  `;

  try {
    await pool.query(sql, [currentDate, currentDate, currentDate]);
    console.log('✅ Insert ke table sqm_hsi berhasil');
  } catch (err) {
    console.error('❌ Error insert sqm_hsi:', err.message);
  }
}

async function insert_ccm_group1(table) {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO ${table} (tgl, jenis, regional, comply)
    SELECT tgl, 'area_ccm', 'BALI NUSRA', ROUND(AVG(NULLIF(comply, '-')), 2) FROM ${table}
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT') GROUP BY tgl
    UNION ALL
    SELECT tgl, 'area_ccm', 'JAWA TIMUR', ROUND(AVG(NULLIF(comply, '-')), 2) FROM ${table}
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('MADIUN','MALANG','JEMBER','SIDOARJO','SURABAYA SELATAN','SURABAYA UTARA','MADURA','PASURUAN') GROUP BY tgl
    UNION ALL
    SELECT tgl, 'area_ccm', 'JATENG DIY', ROUND(AVG(NULLIF(comply, '-')), 2) FROM ${table}
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('KUDUS','MAGELANG','PEKALONGAN','PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA') GROUP BY tgl
  `;

  try {
    await pool.query(sql, [currentDate, currentDate, currentDate]);
    console.log(`✅ Insert ke table ${table} berhasil`);
  } catch (err) {
    console.error(`❌ Error insert ${table}:`, err.message);
  }
}

async function main() {
  try {
    await deleteExistingData();
    await sqm_datin();
    await sqm_hsi();
    await insert_ccm_group1('unspec_datin');
    await insert_ccm_group1('unspec_hsi');
    await insert_ccm_group1('q_datin');
    await insert_ccm_group1('q_hsi');
    console.log('🏁 Semua proses aggregasi Area CCM selesai.');
  } catch (err) {
    console.error('💥 Error Fatal di Main:', err.message);
  } finally {
    // Jika script mandiri (cron), aktifkan baris di bawah:
    await pool.end();
  }
}

main();
