const connection = require('./connection');
const { insertDate } = require('./currentDate');

// ================= DELETE DATA =================
async function deleteExistingData() {
  const tableForDelete = ['ff_ih', 'ff_hsi'];
  const jenisForDelete = ['balnus_ccm', 'jateng_ccm', 'jatim_ccm', 'area_ccm', 'tif'];
  const inPlaceholders = jenisForDelete.map(() => '?').join(',');

  for (const table of tableForDelete) {
    const sql = `
      DELETE FROM ${table}
      WHERE tgl = ?
      AND jenis IN (${inPlaceholders})
    `;
    await connection.execute(sql, [insertDate, ...jenisForDelete]);
  }
}

// ================= TTR DATIN =================
async function ff_ih() {
  const sql = `
    INSERT INTO ff_ih (tgl, jenis, lokasi, ttic, ffg, ttr_ffg)

    SELECT
      ? as tgl,
      rsb.area as jenis,
      rcm.region as lokasi,
      ROUND(SUM(hf.tti_comply) / SUM(hf.tti_ps_ih) * 100, 2) as ttic,
      ROUND(SUM(hf.ffg_comply) / SUM(hf.ffg_jml_ps) * 100, 2) as ffg,
      ROUND(SUM(hf.ttr_ffg_comply) / SUM(hf.ttr_ffg_ggn_wsa) * 100, 2) as ttr_ffg
    FROM region_ccm rcm
    LEFT JOIN helper_fulfillment hf ON hf.lokasi = rcm.sto
    LEFT JOIN region_sub_branch rsb ON rsb.lokasi = rcm.region
    GROUP BY rcm.region, rsb.area

    UNION ALL 

    SELECT
      ? as tgl,
      rsb.area as jenis,
      rcm.branch as lokasi,
      ROUND(SUM(hf.tti_comply) / SUM(hf.tti_ps_ih) * 100, 2) as ttic,
      ROUND(SUM(hf.ffg_comply) / SUM(hf.ffg_jml_ps) * 100, 2) as ffg,
      ROUND(SUM(hf.ttr_ffg_comply) / SUM(hf.ttr_ffg_ggn_wsa) * 100, 2) as ttr_ffg
    FROM region_ccm rcm
    LEFT JOIN helper_fulfillment hf ON hf.lokasi = rcm.sto
    LEFT JOIN region_sub_branch rsb ON rsb.lokasi = rcm.branch
    GROUP BY rcm.branch, rsb.area

    UNION ALL

    SELECT
      ? as tgl,
      'tif' as jenis,
      
      CASE
        WHEN lokasi LIKE 'TERRITORY %' THEN
            CONCAT(
                'TERRITORY ',
                LPAD(SUBSTRING_INDEX(lokasi, ' ', -1), 2, '0')
            )
        ELSE lokasi
      END AS lokasi,
      
      ROUND(tti_real ,2) as ttic,
      ROUND(ffg_real ,2) as ffg,
      ROUND(ttr_ffg_real, 2) as ttr_ffg
      
    FROM helper_fulfillment WHERE lokasi LIKE '%TERRITORY%'


    ORDER BY jenis, lokasi;
  `;

  await connection.execute(sql, [insertDate, insertDate, insertDate]);
  console.log('✅ Insert ke ff_ih berhasil');
}

async function ff_hsi() {
  const sql = `
    INSERT INTO ff_hsi (tgl, jenis, lokasi, ttic, ffg, pspi, ttr_ffg, unspec)

    SELECT
      ? as tgl,
      rsb.area as jenis,
      rcm.region as lokasi,
      ROUND(SUM(hf.tti_comply) / SUM(hf.tti_jml_ps) * 100, 2) as ttic,
      
      ROUND(
        ( SUM(hf.ffg_avg_ps) - SUM(hf.ffg_not_comply) ) / SUM(hf.ffg_avg_ps) * 100
      , 2) as ffg,
      
      ROUND(SUM(hf.pspi_jml_ps) / SUM(hf.pspi_jml_pi) * 100, 2) as pspi,
      ROUND(SUM(hf.ttr_ffg_comply) / SUM(hf.ttr_ffg_jml_ggn) * 100, 2) as ttr_ffg,
      ROUND(
        ( SUM(hf.unspec_jml) - SUM(hf.unspec) ) / SUM(hf.unspec_jml) * 100
      , 2) as unspec
    FROM region_ccm rcm
    LEFT JOIN helper_fulfillment_hsi hf ON hf.lokasi = rcm.sto
    LEFT JOIN region_sub_branch rsb ON rsb.lokasi = rcm.region
    GROUP BY rcm.region, rsb.area

    UNION ALL 


    SELECT
      ? as tgl,
      rsb.area as jenis,
      rcm.branch as lokasi,
      ROUND(SUM(hf.tti_comply) / SUM(hf.tti_jml_ps) * 100, 2) as ttic,
      
      ROUND(
        ( SUM(hf.ffg_avg_ps) - SUM(hf.ffg_not_comply) ) / SUM(hf.ffg_avg_ps) * 100
      , 2) as ffg,
      
      ROUND(SUM(hf.pspi_jml_ps) / SUM(hf.pspi_jml_pi) * 100, 2) as pspi,
      ROUND(SUM(hf.ttr_ffg_comply) / SUM(hf.ttr_ffg_jml_ggn) * 100, 2) as ttr_ffg,
      ROUND(
        ( SUM(hf.unspec_jml) - SUM(hf.unspec) ) / SUM(hf.unspec_jml) * 100
      , 2) as unspec
    FROM region_ccm rcm
    LEFT JOIN helper_fulfillment_hsi hf ON hf.lokasi = rcm.sto
    LEFT JOIN region_sub_branch rsb ON rsb.lokasi = rcm.branch
    GROUP BY rcm.branch, rsb.area

    UNION ALL


    SELECT
      ? as tgl,
      'tif' as jenis,
      
      CASE
        WHEN lokasi LIKE 'TREG %' THEN
            CONCAT(
                'TERRITORY ',
                LPAD(SUBSTRING_INDEX(lokasi, ' ', -1), 2, '0')
            )
        ELSE lokasi
      END AS lokasi,
      
      ROUND(tti_real ,2) as ttic,
      ROUND(ffg_real ,2) as ffg,
      ROUND(pspi_real, 2) as pspi,
      ROUND(ttr_ffg_real, 2) as ttr_ffg,
      ROUND(unspec_real, 2) as unpsec
      
    FROM helper_fulfillment_hsi WHERE lokasi LIKE '%TREG%' AND lokasi != 'TREG 5'
    ORDER BY jenis, lokasi;
  `;

  await connection.execute(sql, [insertDate, insertDate, insertDate]);
  console.log('✅ Insert ke ff_hsi berhasil');
}

async function piloting_fulfillment() {
  const sql = `
    INSERT INTO piloting_fulfillment_cx (
      periode, tgl, piloting, sto, 
      tti_com, tti_tot, ttic, ach_ttic, ttic_comply, 
      ffg_com, ffg_tot, ffg, ach_ffg, ffg_comply, 
      tfg_com, tfg_tot, ttr_ffg, ach_tfg, tfg_comply
    )
    SELECT * FROM (
      /* ================= DETAIL PER STO ================= */
      SELECT
        ? AS periode,
        ? AS tgl,
        rcm.piloting,
        rcm.sto,

        -- TTI Section
        kemt.tti_comply AS tti_com,
        kemt.tti_ps_ih AS tti_tot,
        ROUND((kemt.tti_comply / NULLIF(kemt.tti_ps_ih, 0)) * 100, 2) AS ttic,
        ROUND(((kemt.tti_comply / NULLIF(kemt.tti_ps_ih, 0)) * 100) / 92.72 * 100, 2) AS ach_ttic,
        CASE WHEN (((kemt.tti_comply / NULLIF(kemt.tti_ps_ih, 0)) * 100) / 92.72 * 100) < 100 THEN 'not_comply' END AS ttic_comply,

        -- FFG Section
        kemt.ffg_comply AS ffg_com,
        kemt.ffg_jml_ps AS ffg_tot,
        ROUND((kemt.ffg_comply / NULLIF(kemt.ffg_jml_ps, 0)) * 100, 2) AS ffg,
        ROUND(((kemt.ffg_comply / NULLIF(kemt.ffg_jml_ps, 0)) * 100) / 97.40 * 100, 2) AS ach_ffg,
        CASE WHEN (((kemt.ffg_comply / NULLIF(kemt.ffg_jml_ps, 0)) * 100) / 97.40 * 100) < 100 THEN 'not_comply' END AS ffg_comply,

        -- TFG Section
        kemt.ttr_ffg_comply AS tfg_com,
        kemt.ttr_ffg_ggn_wsa AS tfg_tot,
        ROUND((kemt.ttr_ffg_comply / NULLIF(kemt.ttr_ffg_ggn_wsa, 0)) * 100, 2) AS ttr_ffg,
        ROUND(((kemt.ttr_ffg_comply / NULLIF(kemt.ttr_ffg_ggn_wsa, 0)) * 100) / 80.81 * 100, 2) AS ach_tfg,
        CASE WHEN (((kemt.ttr_ffg_comply / NULLIF(kemt.ttr_ffg_ggn_wsa, 0)) * 100) / 80.81 * 100) < 100 THEN 'not_comply' END AS tfg_comply

      FROM region_ccm rcm
      LEFT JOIN helper_fulfillment kemt ON kemt.lokasi = rcm.sto
      WHERE rcm.piloting IS NOT NULL

      UNION ALL 

      /* ================= TOTAL PER PILOTING ================= */
      SELECT
        ? AS periode,
        ? AS tgl,
        rcm.piloting,
        'TOTAL' AS sto,

        SUM(kemt.tti_comply),
        SUM(kemt.tti_ps_ih),
        ROUND((SUM(kemt.tti_comply) / NULLIF(SUM(kemt.tti_ps_ih), 0)) * 100, 2),
        ROUND(((SUM(kemt.tti_comply) / NULLIF(SUM(kemt.tti_ps_ih), 0)) * 100) / 92.72 * 100, 2),
        CASE WHEN (((SUM(kemt.tti_comply) / NULLIF(SUM(kemt.tti_ps_ih), 0)) * 100) / 92.72 * 100) < 100 THEN 'not_comply' END,

        SUM(kemt.ffg_comply),
        SUM(kemt.ffg_jml_ps),
        ROUND((SUM(kemt.ffg_comply) / NULLIF(SUM(kemt.ffg_jml_ps), 0)) * 100, 2),
        ROUND(((SUM(kemt.ffg_comply) / NULLIF(SUM(kemt.ffg_jml_ps), 0)) * 100) / 97.40 * 100, 2),
        CASE WHEN (((SUM(kemt.ffg_comply) / NULLIF(SUM(kemt.ffg_jml_ps), 0)) * 100) / 97.40 * 100) < 100 THEN 'not_comply' END,

        SUM(kemt.ttr_ffg_comply),
        SUM(kemt.ttr_ffg_ggn_wsa),
        ROUND((SUM(kemt.ttr_ffg_comply) / NULLIF(SUM(kemt.ttr_ffg_ggn_wsa), 0)) * 100, 2),
        ROUND(((SUM(kemt.ttr_ffg_comply) / NULLIF(SUM(kemt.ttr_ffg_ggn_wsa), 0)) * 100) / 80.81 * 100, 2),
        CASE WHEN (((SUM(kemt.ttr_ffg_comply) / NULLIF(SUM(kemt.ttr_ffg_ggn_wsa), 0)) * 100) / 80.81 * 100) < 100 THEN 'not_comply' END
        
      FROM region_ccm rcm
      LEFT JOIN helper_fulfillment kemt ON kemt.lokasi = rcm.sto
      WHERE rcm.piloting IS NOT NULL
      GROUP BY rcm.piloting
    ) X
    ORDER BY piloting, CASE WHEN sto = 'TOTAL' THEN 2 ELSE 1 END, sto;
  `;

  try {
    const params = [yearMonth, currentDate, yearMonth, currentDate];
    await pool.query(sql, params);
    console.log('✅ Insert piloting_fulfillment_cx berhasil');
  } catch (err) {
    console.error('❌ Error piloting_fulfillment:', err.message);
  }
}

// ================= MAIN =================
async function main() {
  try {
    await deleteExistingData();
    await ff_ih();
    await ff_hsi();
    await piloting_fulfillment();
  } catch (err) {
    console.error('❌ Error:', err);
  } finally {
    await connection.end();
  }
}

main();
