const pool = require('./connection'); // Pastikan ini path ke file pool Anda
const { insertDate } = require('./currentDate');

const currentDate = insertDate;
const yearMonth = currentDate.slice(0, 7).replace('-', '');
const likePeriode = `%${yearMonth}%`;

const dateObj = new Date(insertDate);
const month = String(dateObj.getMonth() + 1).padStart(2, '0');

console.log(`Bulan: ${month}, Periode: ${yearMonth}, Like: ${likePeriode}`);

// Fungsi hapus data berdasarkan periode
async function deleteExistingData() {
  const tableForDelete = ['piloting_n2n_cx_service_sugar'];
  for (const table of tableForDelete) {
    const sql = `DELETE FROM ${table} WHERE periode = ?`;
    // Langsung gunakan pool.query (sudah mendukung promise)
    await pool.query(sql, [yearMonth]);
  }
}

async function piloting_service_sugar() {
  // Gunakan template literal untuk m${month} agar dinamis
  const sql = `
    INSERT INTO perf_tif.piloting_n2n_cx_service_sugar
    (periode, tgl, piloting, sto,
     service_lb, service_lg, service, ach_service, service_comp,
     sugar_lb, sugar_lg, sugar, ach_sugar, sugar_comp)

    SELECT *
    FROM (
      -- ================= DETAIL PER STO =================
      SELECT
        ? AS periode,
        ? AS tgl,
        rcm.piloting,
        rcm.sto,

        CAST(hslb.m${month} AS UNSIGNED) AS service_lb,
        CAST(hslg.m${month} AS UNSIGNED) AS service_lg,

        ROUND(
          (CAST(hslb.m${month} AS SIGNED) - CAST(hslg.m${month} AS SIGNED))
          / NULLIF(CAST(hslb.m${month} AS SIGNED), 0) * 100, 2
        ) AS service,

        ROUND(
          (
            (CAST(hslb.m${month} AS SIGNED) - CAST(hslg.m${month} AS SIGNED))
            / NULLIF(CAST(hslb.m${month} AS SIGNED), 0) * 100
          ) / 98.52 * 100, 2
        ) AS ach_service,

        CASE
          WHEN (
            (CAST(hslb.m${month} AS SIGNED) - CAST(hslg.m${month} AS SIGNED))
            / NULLIF(CAST(hslb.m${month} AS SIGNED), 0) * 100
          ) / 98.52 * 100 < 100 THEN 'not_comply'
          ELSE 'comply'
        END AS service_comp,

        CAST(hsulb.m${month} AS UNSIGNED) AS sugar_lb,
        CAST(hsulg.m${month} AS UNSIGNED) AS sugar_lg,

        ROUND(
          (CAST(hsulb.m${month} AS SIGNED) - CAST(hsulg.m${month} AS SIGNED))
          / NULLIF(CAST(hsulb.m${month} AS SIGNED), 0) * 100, 2
        ) AS sugar,

        ROUND(
          (
            (CAST(hsulb.m${month} AS SIGNED) - CAST(hsulg.m${month} AS SIGNED))
            / NULLIF(CAST(hsulb.m${month} AS SIGNED), 0) * 100
          ) / 91.71 * 100, 2
        ) AS ach_sugar,

        CASE
          WHEN (
            (CAST(hsulb.m${month} AS SIGNED) - CAST(hsulg.m${month} AS SIGNED))
            / NULLIF(CAST(hsulb.m${month} AS SIGNED), 0) * 100
          ) / 91.71 * 100 < 100 THEN 'not_comply'
          ELSE 'comply'
        END AS sugar_comp

      FROM perf_tif.region_ccm rcm
      LEFT JOIN perf_tif.helper_service_list_berbil hslb ON hslb.witel = rcm.sto
      LEFT JOIN perf_tif.helper_service_list_gangguan hslg ON hslg.witel = rcm.sto
      LEFT JOIN perf_tif.helper_sugar_list_berbil hsulb ON hsulb.witel = rcm.sto
      LEFT JOIN perf_tif.helper_sugar_list_gangguan hsulg ON hsulg.witel = rcm.sto
      WHERE rcm.piloting IS NOT NULL

      UNION ALL

      -- ================= TOTAL PER PILOTING =================
      SELECT
        ? AS periode,
        ? AS tgl,
        rcm.piloting,
        'TOTAL',

        SUM(CAST(hslb.m${month} AS UNSIGNED)),
        SUM(CAST(hslg.m${month} AS UNSIGNED)),

        ROUND(
          (SUM(CAST(hslb.m${month} AS SIGNED)) - SUM(CAST(hslg.m${month} AS SIGNED)))
          / NULLIF(SUM(CAST(hslb.m${month} AS SIGNED)), 0) * 100, 2
        ),

        ROUND(
          (
            (SUM(CAST(hslb.m${month} AS SIGNED)) - SUM(CAST(hslg.m${month} AS SIGNED)))
            / NULLIF(SUM(CAST(hslb.m${month} AS SIGNED)), 0) * 100
          ) / 98.52 * 100, 2
        ),

        CASE
          WHEN (
            (SUM(CAST(hslb.m${month} AS SIGNED)) - SUM(CAST(hslg.m${month} AS SIGNED)))
            / NULLIF(SUM(CAST(hslb.m${month} AS SIGNED)), 0) * 100
          ) / 98.52 * 100 < 100 THEN 'not_comply'
          ELSE 'comply'
        END,

        SUM(CAST(hsulb.m${month} AS UNSIGNED)),
        SUM(CAST(hsulg.m${month} AS UNSIGNED)),

        ROUND(
          (SUM(CAST(hsulb.m${month} AS SIGNED)) - SUM(CAST(hsulg.m${month} AS SIGNED)))
          / NULLIF(SUM(CAST(hsulb.m${month} AS SIGNED)), 0) * 100, 2
        ),

        ROUND(
          (
            (SUM(CAST(hsulb.m${month} AS SIGNED)) - SUM(CAST(hsulg.m${month} AS SIGNED)))
            / NULLIF(SUM(CAST(hsulb.m${month} AS SIGNED)), 0) * 100
          ) / 91.71 * 100, 2
        ),

        CASE
          WHEN (
            (SUM(CAST(hsulb.m${month} AS SIGNED)) - SUM(CAST(hsulg.m${month} AS SIGNED)))
            / NULLIF(SUM(CAST(hsulb.m${month} AS SIGNED)), 0) * 100
          ) / 91.71 * 100 < 100 THEN 'not_comply'
          ELSE 'comply'
        END

      FROM perf_tif.region_ccm rcm
      LEFT JOIN perf_tif.helper_service_list_berbil hslb ON hslb.witel = rcm.sto
      LEFT JOIN perf_tif.helper_service_list_gangguan hslg ON hslg.witel = rcm.sto
      LEFT JOIN perf_tif.helper_sugar_list_berbil hsulb ON hsulb.witel = rcm.sto
      LEFT JOIN perf_tif.helper_sugar_list_gangguan hsulg ON hsulg.witel = rcm.sto
      WHERE rcm.piloting IS NOT NULL
      GROUP BY rcm.piloting
    ) x
    ORDER BY piloting, CASE WHEN sto = 'TOTAL' THEN 2 ELSE 1 END, sto;
  `;

  const params = [
    yearMonth,
    currentDate, // detail
    yearMonth,
    currentDate, // total
  ];

  await pool.query(sql, params);
  console.log('✅ Insert piloting_n2n_cx_service_sugar berhasil');
}

async function main() {
  try {
    await deleteExistingData();
    await piloting_service_sugar();
  } catch (err) {
    console.error('❌ Error:', err.message);
  } finally {
    // Jika script ini cron job yang berhenti setelah selesai, gunakan:
    await pool.end();
    // Jika script ini bagian dari web server, jangan panggil .end()
  }
}

main();
