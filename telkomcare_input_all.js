const connection = require('./connection');
const { insertDate } = require('./currentDate');
const path = require('path');

// ================= DELETE DATA =================
async function deleteExistingData() {
  const tableForDelete = ['ttr_datin', 'sugar_datin', 'hsi_sugar', 'ttr_indibiz', 'ttr_reseller', 'ttr_siptrunk'];
  const jenisForDelete = ['balnus_ccm', 'jateng_ccm', 'jatim_ccm', 'area_ccm'];
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
async function ttr_datin() {
  const sql = `
    INSERT INTO ttr_datin (jenis, treg, tgl, k1, k2, k3)
    SELECT
      rsb.area AS jenis,
      rcm.branch AS treg,
      ? AS tgl,
      'nan' AS k1,
      ROUND(
        COUNT(CASE WHEN htd.KATEGORI = 'K2' AND htd.COMPLIANCE = 'COMPLY' THEN 1 END) / NULLIF(COUNT(CASE WHEN htd.KATEGORI = 'K2' THEN 1 END), 0) * 100,
        2
      ) AS k2,
      ROUND(
        COUNT(CASE WHEN htd.KATEGORI = 'K3' AND htd.COMPLIANCE = 'COMPLY' THEN 1 END) / NULLIF(COUNT(CASE WHEN htd.KATEGORI = 'K3' THEN 1 END), 0) * 100,
        2
      ) AS k3
    FROM
      region_ccm rcm
      LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
      LEFT JOIN helper_ttr_datin htd ON htd.WORKZONE = rcm.sto
    GROUP BY
      rcm.branch
      
    UNION ALL 

    SELECT
      'area_ccm' AS jenis,
      rcm.region AS treg,
      ? AS tgl,
      'nan' AS k1,
      ROUND(
        COUNT(CASE WHEN htd.KATEGORI = 'K2' AND htd.COMPLIANCE = 'COMPLY' THEN 1 END) / NULLIF(COUNT(CASE WHEN htd.KATEGORI = 'K2' THEN 1 END), 0) * 100,
        2
      ) AS k2,
      ROUND(
        COUNT(CASE WHEN htd.KATEGORI = 'K3' AND htd.COMPLIANCE = 'COMPLY' THEN 1 END) / NULLIF(COUNT(CASE WHEN htd.KATEGORI = 'K3' THEN 1 END), 0) * 100,
        2
      ) AS k3
    FROM
      region_ccm rcm
      LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
      LEFT JOIN helper_ttr_datin htd ON htd.WORKZONE = rcm.sto
    GROUP BY
      rcm.region
  `;

  await connection.execute(sql, [insertDate, insertDate]);
  console.log('✅ Insert ke ttr_datin berhasil');
}

// ================= SUGAR DATIN =================
async function sugar_datin() {
  const sql = `
    INSERT INTO sugar_datin (jenis, treg, tgl, \`real\`)
    SELECT
      rsb.area AS jenis,
      rcm.branch AS treg,
      ? AS tgl,
      ROUND(
        COUNT(CASE WHEN htd.COMPLIANCE = 'COMPLY' THEN 1 END)
        /
        NULLIF(COUNT(htd.INCIDENT),0) * 100
      ,2)
    FROM region_ccm rcm
    LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
    LEFT JOIN helper_sugar_datin htd ON htd.WORKZONE = rcm.sto
    GROUP BY rcm.branch, rsb.area

    UNION ALL 

    SELECT
      'area_ccm' AS jenis,
      rcm.region AS treg,
      ? AS tgl,
      ROUND(
        COUNT(CASE WHEN htd.COMPLIANCE = 'COMPLY' THEN 1 END) / NULLIF(COUNT(htd.INCIDENT), 0) * 100,
        2
      )
    FROM
      region_ccm rcm
      LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
      LEFT JOIN helper_sugar_datin htd ON htd.WORKZONE = rcm.sto
    GROUP BY
      rcm.region
  `;

  await connection.execute(sql, [insertDate, insertDate]);
  console.log('✅ Insert ke sugar_datin berhasil');
}

// ================= SUGAR HSI =================
async function sugar_hsi() {
  const sql = `
    INSERT INTO hsi_sugar (jenis, treg, tgl, \`real\`)
    SELECT
      rsb.area AS jenis,
      rcm.branch AS treg,
      ? AS tgl,
      ROUND(
        ( NULLIF(COUNT(htd.INCIDENT),0) - COUNT(CASE WHEN htd.COMPLIANCE = 'COMPLY' THEN 1 END) ) / NULLIF(COUNT(htd.INCIDENT),0) * 100
      ,2)
    FROM region_ccm rcm
    LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
    LEFT JOIN helper_sugar_hsi htd ON htd.WORKZONE = rcm.sto
    GROUP BY rcm.branch, rsb.area

    UNION ALL

    SELECT
      'area_ccm' AS jenis,
      rcm.region AS treg,
      ? AS tgl,
      ROUND(
        (NULLIF(COUNT(htd.INCIDENT), 0) - COUNT(CASE WHEN htd.COMPLIANCE = 'COMPLY' THEN 1 END)) / NULLIF(COUNT(htd.INCIDENT), 0) * 100,
        2
      )
    FROM
      region_ccm rcm
      LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
      LEFT JOIN helper_sugar_hsi htd ON htd.WORKZONE = rcm.sto
    GROUP BY
      rcm.region
  `;

  await connection.execute(sql, [insertDate, insertDate]);
  console.log('✅ Insert ke sugar_hsi berhasil');
}

// ================= TTR INDIBIZ =================
async function ttr_indibiz() {
  const sql = `
    INSERT INTO ttr_indibiz (jenis, treg, tgl, real_1, real_2)
    SELECT
      rsb.area AS jenis,
      rcm.branch AS treg,
      ? AS tgl,
      ROUND(
        COUNT(CASE WHEN htd.COMPLY_4_E2E = 'COMPLY' THEN 1 END)
        /
        NULLIF(COUNT(htd.INCIDENT),0) * 100
      ,2),
      ROUND(
        COUNT(CASE WHEN htd.COMPLY_24_E2E = 'COMPLY' THEN 1 END)
        /
        NULLIF(COUNT(htd.INCIDENT),0) * 100
      ,2)
    FROM region_ccm rcm
    LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
    LEFT JOIN helper_ttr_indibiz htd ON htd.WORKZONE = rcm.sto
    GROUP BY rcm.branch

    UNION ALL 
  
    SELECT
      'area_ccm' AS jenis,
      rcm.region AS treg,
      ? AS tgl,
      ROUND(
        COUNT(CASE WHEN htd.COMPLY_4_E2E = 'COMPLY' THEN 1 END) / NULLIF(COUNT(htd.INCIDENT), 0) * 100,
        2
      ),
      ROUND(
        COUNT(CASE WHEN htd.COMPLY_24_E2E = 'COMPLY' THEN 1 END) / NULLIF(COUNT(htd.INCIDENT), 0) * 100,
        2
      )
    FROM
      region_ccm rcm
      LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
      LEFT JOIN helper_ttr_indibiz htd ON htd.WORKZONE = rcm.sto
    GROUP BY
      rcm.region
  `;

  await connection.execute(sql, [insertDate, insertDate]);
  console.log('✅ Insert ke ttr_reseller berhasil');
}

// ================= TTR RESELLER =================
async function ttr_reseller() {
  const sql = `
    INSERT INTO ttr_reseller (jenis, treg, tgl, real_1, real_2)
    SELECT
      rsb.area AS jenis,
      rcm.branch AS treg,
      ? AS tgl,
      ROUND(
        COUNT(CASE WHEN htd.COMPLY_6_E2E = 'COMPLY' THEN 1 END)
        /
        NULLIF(COUNT(htd.INCIDENT),0) * 100
      ,2),
      ROUND(
        COUNT(CASE WHEN htd.COMPLY_36_E2E = 'COMPLY' THEN 1 END)
        /
        NULLIF(COUNT(htd.INCIDENT),0) * 100
      ,2)
    FROM region_ccm rcm
    LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
    LEFT JOIN helper_ttr_reseller htd ON htd.WORKZONE = rcm.sto
    GROUP BY rcm.branch

    UNION ALL 

    SELECT
      'area_ccm' AS jenis,
      rcm.region AS treg,
      ? AS tgl,
      ROUND(
        COUNT(CASE WHEN htd.COMPLY_6_E2E = 'COMPLY' THEN 1 END) / NULLIF(COUNT(htd.INCIDENT), 0) * 100,
        2
      ),
      ROUND(
        COUNT(CASE WHEN htd.COMPLY_36_E2E = 'COMPLY' THEN 1 END) / NULLIF(COUNT(htd.INCIDENT), 0) * 100,
        2
      )
    FROM
      region_ccm rcm
      LEFT JOIN region_sub_branch rsb ON rcm.branch = rsb.lokasi
      LEFT JOIN helper_ttr_reseller htd ON htd.WORKZONE = rcm.sto
    GROUP BY
      rcm.region
  `;

  await connection.execute(sql, [insertDate, insertDate]);
  console.log('✅ Insert ke ttr_reseller berhasil');
}

async function ttr_siptrunk() {
  const sql = `
    INSERT INTO ttr_siptrunk (jenis, treg, tgl, target, \`real\`, ach)

    SELECT
      'area_ccm' as jenis,
      rcm.region as treg,
      ? as tgl,
      10 as target,
      CASE 
        WHEN AVG(hs.TTR_E2E) IS NULL THEN '-'
        ELSE ROUND(AVG(hs.TTR_E2E), 2)
      END AS realisasi,
      
      CASE 
        WHEN AVG(hs.TTR_E2E) IS NULL 
        THEN 100
        ELSE ROUND(
              CASE 
                WHEN (10 / AVG(hs.TTR_E2E)) > 1 
                THEN 100 
                ELSE (10 / AVG(hs.TTR_E2E) * 100)
              END
            ,2)
      END AS ach
      
    FROM
      region_ccm rcm
    LEFT JOIN helper_siptrunk hs ON hs.WORKZONE = rcm.sto
    GROUP BY rcm.region

    UNION ALL 

    SELECT
      rsb.area as jenis,
      rcm.branch as treg,
      ? as tgl,
      10 as target,
      CASE 
        WHEN AVG(hs.TTR_E2E) IS NULL THEN '-'
        ELSE ROUND(AVG(hs.TTR_E2E), 2)
      END AS realisasi,
      
      CASE 
        WHEN AVG(hs.TTR_E2E) IS NULL 
        THEN 100
        ELSE ROUND(
              CASE 
                WHEN (10 / AVG(hs.TTR_E2E)) > 1 
                THEN 100 
                ELSE (10 / AVG(hs.TTR_E2E) * 100)
              END
            ,2)
      END AS ach
    FROM
      region_ccm rcm
    LEFT JOIN helper_siptrunk hs ON hs.WORKZONE = rcm.sto
    LEFT JOIN region_sub_branch rsb ON rsb.lokasi = rcm.branch
    GROUP BY rcm.branch

    ORDER BY jenis,treg

  `;

  await connection.execute(sql, [insertDate, insertDate]);
  console.log('✅ Insert ke ttr_siptrunk berhasil');
}

// ================= MAIN =================
async function main() {
  try {
    await deleteExistingData();
    await ttr_datin();
    await sugar_datin();
    await sugar_hsi();
    await ttr_indibiz();
    await ttr_reseller();
    await ttr_siptrunk();
  } catch (err) {
    console.error('❌ Error:', err);
  } finally {
    await connection.end();
  }
}

main();
