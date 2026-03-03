const pool = require('./connection'); // Menggunakan pool promise
const { insertDate } = require('./currentDate');

const currentDate = insertDate;
const yearMonth = currentDate.slice(0, 7).replace('-', '');
const likePeriode = `%${yearMonth}%`;

/**
 * Fungsi hapus data lama agar tidak duplikat saat insert ulang
 */
async function deleteExistingData() {
  const tableForDelete = ['piloting_n2n_cx'];
  for (const table of tableForDelete) {
    try {
      const sql = `DELETE FROM ${table} WHERE periode = ?`;
      await pool.query(sql, [yearMonth]);
      console.log(`🗑️ Data lama di ${table} periode ${yearMonth} telah dihapus.`);
    } catch (err) {
      console.error(`❌ Gagal menghapus data di ${table}:`, err.message);
    }
  }
}

/**
 * Fungsi utama Agregasi Piloting TTR
 */
async function piloting_ttr() {
  console.log(`🚀 Memproses Piloting TTR: ${yearMonth} | ${currentDate}`);

  const sql = `
    INSERT INTO perf_tif.piloting_n2n_cx (
      periode, tgl, piloting, sto, 
      com3, tot3, real_com3, ach_com3, comply_3, 
      com6, tot6, real_com6, ach_com6, comply_6, 
      com12, tot12, real_com12, ach_com12, comply_12, 
      com24, tot24, real_com24, ach_com24, comply_24, 
      com36, tot36, real_com36, ach_com36, comply_36
    )
    SELECT * FROM (
      /* ================= DETAIL PER STO ================= */
      SELECT
        ? AS periode,
        ? AS tgl,
        rcm.piloting AS piloting,
        dwc.STO AS sto,
        
        -- COMPLY 3 DAYS (DIAMOND)
        COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) AS com3,
        COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) AS tot3,
        ROUND((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END), 0) * 100), 2) AS real_com3,
        ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END), 0) * 100), 2) / 95.25) * 100, 2) AS ach_com3,
        CASE WHEN (ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END), 0) * 100), 2) / 95.25) * 100, 2)) < 100 THEN 'not_comply' END AS comply_3,

        -- COMPLY 6 DAYS (PLATINUM)
        COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) AS com6,
        COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) AS tot6,
        ROUND((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END), 0) * 100), 2) AS real_com6,
        ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END), 0) * 100), 2) / 95) * 100, 2) AS ach_com6,
        CASE WHEN (ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END), 0) * 100), 2) / 95) * 100, 2)) < 100 THEN 'not_comply' END AS comply_6,

        -- COMPLY 12 DAYS (GOLD)
        COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) AS com12,
        COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) AS tot12,
        ROUND((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END), 0) * 100), 2) AS real_com12,
        ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END), 0) * 100), 2) / 83) * 100, 2) AS ach_com12,
        CASE WHEN (ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END), 0) * 100), 2) / 83) * 100, 2)) < 100 THEN 'not_comply' END AS comply_12,

        -- COMPLY 24 DAYS (REGULER)
        COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) AS com24,
        COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END) AS tot24,
        ROUND((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END), 0) * 100), 2) AS real_com24,
        ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END), 0) * 100), 2) / 99) * 100, 2) AS ach_com24,
        CASE WHEN (ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END), 0) * 100), 2) / 99) * 100, 2)) < 100 THEN 'not_comply' END AS comply_24,

        -- COMPLY 36 DAYS (REGULER & GOLD)
        COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) AS com36,
        COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) AS tot36,
        ROUND((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END), 0) * 100), 2) AS real_com36,
        ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END), 0) * 100), 2) / 99.5) * 100, 2) AS ach_com36,
        CASE WHEN (ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END), 0) * 100), 2) / 99.5) * 100, 2)) < 100 THEN 'not_comply' END AS comply_36

      FROM perf_tif.region_ccm rcm
      LEFT JOIN nonatero_download.Detil_WSA_CLOSE dwc
        ON dwc.STO = rcm.STO
        AND dwc.DPER LIKE ?
        AND dwc.IS_KPI_TTR = 1
      WHERE rcm.piloting IS NOT NULL
      GROUP BY rcm.piloting, dwc.STO

      UNION ALL

      /* ================= TOTAL PER PILOTING ================= */
      SELECT
        ? AS periode,
        ? AS tgl,
        rcm.piloting AS piloting,
        'TOTAL' AS sto,
        
        COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END),
        COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END),
        ROUND((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END), 0) * 100), 2),
        ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END), 0) * 100), 2) / 95.25) * 100, 2),
        CASE WHEN (ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY3 = 1 AND dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_DIAMOND' THEN 1 END), 0) * 100), 2) / 95.25) * 100, 2)) < 100 THEN 'not_comply' END,

        COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END),
        COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END),
        ROUND((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END), 0) * 100), 2),
        ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END), 0) * 100), 2) / 95) * 100, 2),
        CASE WHEN (ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY6 = 1 AND dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_PLATINUM' THEN 1 END), 0) * 100), 2) / 95) * 100, 2)) < 100 THEN 'not_comply' END,

        COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END),
        COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END),
        ROUND((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END), 0) * 100), 2),
        ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END), 0) * 100), 2) / 83) * 100, 2),
        CASE WHEN (ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY12 = 1 AND dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'HVC_GOLD' THEN 1 END), 0) * 100), 2) / 83) * 100, 2)) < 100 THEN 'not_comply' END,

        COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END),
        COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END),
        ROUND((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END), 0) * 100), 2),
        ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END), 0) * 100), 2) / 99) * 100, 2),
        CASE WHEN (ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY24 = 1 AND dwc.FLAG_HVC = 'REGULER' THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC = 'REGULER' THEN 1 END), 0) * 100), 2) / 99) * 100, 2)) < 100 THEN 'not_comply' END,

        COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END),
        COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END),
        ROUND((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END), 0) * 100), 2),
        ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END), 0) * 100), 2) / 99.5) * 100, 2),
        CASE WHEN (ROUND((ROUND((COUNT(CASE WHEN dwc.COMPLY36 = 1 AND dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END) / NULLIF(COUNT(CASE WHEN dwc.FLAG_HVC IN ('REGULER', 'HVC_GOLD') THEN 1 END), 0) * 100), 2) / 99.5) * 100, 2)) < 100 THEN 'not_comply' END

      FROM perf_tif.region_ccm rcm
      LEFT JOIN nonatero_download.Detil_WSA_CLOSE dwc
        ON dwc.STO = rcm.STO
        AND dwc.DPER LIKE ?
        AND dwc.IS_KPI_TTR = 1
      WHERE rcm.piloting IS NOT NULL
      GROUP BY rcm.piloting
    ) x
    ORDER BY
      piloting,
      CASE WHEN sto = 'TOTAL' THEN 2 ELSE 1 END,
      sto;
  `;

  try {
    // Urutan Parameter: periode, tgl, likePeriode (Part 1), periode, tgl, likePeriode (Part 2)
    const params = [yearMonth, currentDate, likePeriode, yearMonth, currentDate, likePeriode];
    await pool.query(sql, params);
    console.log('✅ Agregasi Piloting N2N CX berhasil disimpan.');
  } catch (err) {
    console.error('❌ Error insert piloting:', err.message);
  }
}

// === MAIN RUNNER ===
(async () => {
  try {
    await deleteExistingData();
    await piloting_ttr();
  } catch (err) {
    console.error('💥 Fatal Error:', err.message);
  } finally {
    pool.end();
  }
})();
