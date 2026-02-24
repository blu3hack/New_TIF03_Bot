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

// ================= MAIN =================
async function main() {
  try {
    await deleteExistingData();
    await ff_ih();
    await ff_hsi();
  } catch (err) {
    console.error('❌ Error:', err);
  } finally {
    await connection.end();
  }
}

main();
