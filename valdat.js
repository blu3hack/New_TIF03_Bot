const puppeteer = require('puppeteer');
const fs = require('fs');
const connection = require('./connection');
const csv = require('csv-parser');
const { exec } = require('child_process');
const { user_aribi, pass_aribi } = require('./login');

(async () => {
  const browser = await puppeteer.launch({ headless: false }); // Buka browser (non-headless)
  const page = await browser.newPage();

  // Navigasi ke halaman login
  await page.goto('https://access-quality.telkom.co.id/aqi/index.php/login');

  // Screenshoot cpt
  const element = await page.$('body > div.container-fluid > div > div.col-xl-4.p-0 > div > form > div.row > div.col-lg-4 > img'); // ganti #main dengan id target
  if (element) {
    await element.screenshot({
      path: 'captcha/cpt.png',
    });
  } else {
    console.log('? Elemen dengan id tersebut tidak ditemukan');
  }

  // // mbil cpacha dari database
  async function getData() {
    const query = `
        SELECT pesan 
        FROM get_otp_for_download 
        WHERE pesan LIKE '%cpt%' 
        ORDER BY id DESC 
        LIMIT 1
      `;
    const [rows] = await connection.query(query);
    return rows;
  }

  // Isi formulir login
  await page.type('#username', user_aribi);
  await page.type('#password', pass_aribi);

  // input captcha
  await page.waitForTimeout(20000);
  const result = await getData();
  const pesan = result[0].pesan; // contoh: "cpt azp"
  const parts = pesan.split(' ');
  const captcha = parts[1] || null; // ambil kata setelah "cpt"
  console.log(captcha);
  await page.type('[placeholder="Masukan captcha"]', String(captcha));

  await page.waitForTimeout(5000);
  await page.click('body > div.container-fluid > div > div.col-xl-4.p-0 > div > form > div:nth-child(10) > button');

  async function getOTP() {
    return new Promise((resolve, reject) => {
      exec('python otp_valdat.py', (error, stdout, stderr) => {
        if (error) {
          console.error(`Terjadi kesalahan: ${error.message}`);
          return reject(error);
        }
        if (stderr) {
          console.error(`Stderr: ${stderr}`);
          return reject(stderr);
        }

        const otp = stdout.trim();
        console.log(`OTP dari Python: ${otp}`); // Pastikan OTP valid di sini
        resolve(otp);
      });
    });
  }
  await page.waitForTimeout(3000);

  async function insertOTP() {
    const otp = await getOTP();
    console.log('OTP yang didapat:', otp);
    await page.type('[placeholder="OTP"]', otp);
    await page.click('#submit');
  }
  await insertOTP();

  await page.waitForNavigation();

  // Sekarang Anda dapat mengarahkan browser ke halaman yang Anda inginkan
  await page.goto('https://access-quality.telkom.co.id/aqi/valdat/valdat2026/nasional2026');
  // Mengambil data dari tabel
  const valda_area = await page.evaluate(() => {
    const table = document.querySelector('#pageWrapper > div.page-body-wrapper.null > div.page-body > div:nth-child(2) > div > div > div > div.card-body > div > div > div > table > tbody');
    const rows = Array.from(table.querySelectorAll('tr'));
    return rows
      .map((row) => {
        const columns = Array.from(row.querySelectorAll('td, th'));
        return columns
          .map((column) => {
            let cellValue = column.innerText.trim();
            // Mengganti koma dengan string kosong
            if (cellValue.includes(',')) {
              cellValue = cellValue.replace(/,/g, '');
            }
            return cellValue;
          })
          .join(',');
      })
      .join('\n');
  });

  console.log(`${valda_area} \n \n`);
  fs.writeFileSync('loaded_file/valdat/area.csv', valda_area);

  await page.goto('https://access-quality.telkom.co.id/aqi/valdat/valdat2026/area2026?area=JAWA%20BALI&filter=2026-04-06');

  const valdat_region = await page.evaluate(() => {
    const table = document.querySelector('#pageWrapper > div.page-body-wrapper.null > div.page-body > div:nth-child(2) > div > div > div > div.card-body > div > div > div > table > tbody');
    const rows = Array.from(table.querySelectorAll('tr'));
    return rows
      .map((row) => {
        const columns = Array.from(row.querySelectorAll('td, th'));
        return columns
          .map((column) => {
            let cellValue = column.innerText.trim();
            // Mengganti koma dengan string kosong
            if (cellValue.includes(',')) {
              cellValue = cellValue.replace(/,/g, '');
            }
            return cellValue;
          })
          .join(',');
      })
      .join('\n');
  });

  console.log(`${valdat_region} \n \n`);
  fs.writeFileSync('loaded_file/valdat/region.csv', valdat_region);

  // valdat jatim
  await page.goto('https://access-quality.telkom.co.id/aqi/valdat/valdat2026/regional2026?&area=JAWA%20BALI&regional_name=JATIM&filter=2026-04-06');

  // Mengambil data dari tabel
  const distric_jatim = await page.evaluate(() => {
    const table = document.querySelector('#pageWrapper > div.page-body-wrapper.null > div.page-body > div:nth-child(2) > div > div > div > div.card-body > div > div > div > table > tbody');
    const rows = Array.from(table.querySelectorAll('tr'));
    return rows
      .map((row) => {
        const columns = Array.from(row.querySelectorAll('td, th'));
        return columns
          .map((column) => {
            let cellValue = column.innerText.trim();
            // Mengganti koma dengan string kosong
            if (cellValue.includes(',')) {
              cellValue = cellValue.replace(/,/g, '');
            }
            return cellValue;
          })
          .join(',');
      })
      .join('\n');
  });

  console.log(`${distric_jatim} \n \n`);
  fs.writeFileSync('loaded_file/valdat/jatim.csv', distric_jatim);

  // valdat jateng
  await page.goto('https://access-quality.telkom.co.id/aqi/valdat/valdat2026/regional2026?&area=JAWA%20BALI&regional_name=JATENG%20DIY&filter=2026-04-06');

  // Mengambil data dari tabel
  const distric_jateng = await page.evaluate(() => {
    const table = document.querySelector('#pageWrapper > div.page-body-wrapper.null > div.page-body > div:nth-child(2) > div > div > div > div.card-body > div > div > div > table > tbody');
    const rows = Array.from(table.querySelectorAll('tr'));
    return rows
      .map((row) => {
        const columns = Array.from(row.querySelectorAll('td, th'));
        return columns
          .map((column) => {
            let cellValue = column.innerText.trim();
            // Mengganti koma dengan string kosong
            if (cellValue.includes(',')) {
              cellValue = cellValue.replace(/,/g, '');
            }
            return cellValue;
          })
          .join(',');
      })
      .join('\n');
  });

  console.log(`${distric_jateng} \n \n`);
  fs.writeFileSync('loaded_file/valdat/jateng.csv', distric_jateng);

  // valdat bali
  await page.goto('https://access-quality.telkom.co.id/aqi/valdat/valdat2026/regional2026?&area=JAWA%20BALI&regional_name=BALINUSRA&filter=2026-04-06');

  // Mengambil data dari tabel
  const distric_bali = await page.evaluate(() => {
    const table = document.querySelector('#pageWrapper > div.page-body-wrapper.null > div.page-body > div:nth-child(2) > div > div > div > div.card-body > div > div > div > table > tbody');
    const rows = Array.from(table.querySelectorAll('tr'));
    return rows
      .map((row) => {
        const columns = Array.from(row.querySelectorAll('td, th'));
        return columns
          .map((column) => {
            let cellValue = column.innerText.trim();
            // Mengganti koma dengan string kosong
            if (cellValue.includes(',')) {
              cellValue = cellValue.replace(/,/g, '');
            }
            return cellValue;
          })
          .join(',');
      })
      .join('\n');
  });

  console.log(`${distric_bali} \n \n`);
  fs.writeFileSync('loaded_file/valdat/bali.csv', distric_bali);

  // Tutup browser setelah penundaan
  await browser.close();
})();
