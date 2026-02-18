const fs = require('fs');
const cheerio = require('cheerio');
const path = require('path');

function convertXLSToCSV(fileName) {
  const filePath = `D:/SCRAPPERS/Scrapper/loaded_file/download_fulfillment/${fileName}.xls`;
  // Baca file sebagai teks
  let html = fs.readFileSync(filePath, 'utf8');

  // Bersihkan karakter tidak valid
  html = html.replace(/[\x00-\x09\x0B-\x1F]/g, '');

  // Load HTML
  const $ = cheerio.load(html);

  // Pastikan tabel ada
  const table = $('table');
  if (table.length === 0) {
    console.error('❌ Tidak ada <table> ditemukan dalam file!');
    process.exit();
  }

  let rows = [];

  // Loop baris
  table.find('tr').each((i, tr) => {
    let row = [];

    $(tr)
      .find('td, th')
      .each((j, cell) => {
        let text = $(cell).text().trim();
        text = text.replace(/\s+/g, ' ').trim(); // Normalisasi spasi
        row.push(text);
      });

    if (row.length > 0 && row.some((col) => col !== '')) {
      rows.push(row);
    }
  });

  let csvContent = rows
    .map((r) =>
      r
        .map(
          (col) => `"${col.replace(/"/g, '""')}"`, // escape double quotes
        )
        .join(','),
    )
    .join('\n');

  // Lokasi save CSV
  const csvPath = filePath.replace('.xls', '.csv');
  fs.writeFileSync(csvPath, csvContent);

  console.log(`file ${fileName}.csv berhasil disimpan dengan jumlah baris: ${rows.length}`);
}

convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'TERRITORY3'_''_'DATIN24_-");
convertXLSToCSV("REPORT_SUMMARY_GAUL_'_'TERRITORY3'_''_'HSI24_-");
convertXLSToCSV("REPORT_TIKET_COMPLIANCE24_'_'TERRITORY 3'_''_'DATIN24_-");
convertXLSToCSV("REPORT_TIKET_COMPLIANCE24_'_'TERRITORY 3'_''_'INDIBIZ_-");
convertXLSToCSV("REPORT_TIKET_COMPLIANCE24_'_'TERRITORY 3'_''_'RESELLER_-");
// convertXLSToCSV('all-cnop3-ticket');
