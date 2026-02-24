const puppeteer = require('puppeteer');
const { user_wifi, pass_wifi } = require('./login');
const fs = require('fs');
const { startdate_long_format, enddate_long_format } = require('./currentDate');
(async () => {
  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();
  // Navigasi ke halaman login
  await page.goto('https://smarthub.wifi.id/website/index.php/login');
  // Isi formulir login
  await page.type('[placeholder="Username"]', user_wifi);
  await page.type('[placeholder="Password"]', pass_wifi);
  await page.click('body > div:nth-child(1) > div > div > div > form > div:nth-child(3) > button');
  console.log('Berhasil Login');

  // Fungsi umum untuk mengambil data tabel
  async function getDataFromTable(selector, nameFile) {
    // Cek apakah nameFile mengandung 'sugar'
    const skipDate = nameFile.includes('sugar');

    if (!skipDate) {
      // startdate
      const startdate = startdate_long_format;
      console.log(startdate);

      const startSelector = 'body > div > section > div > div:nth-child(3) > div > div > div > form > div > div:nth-child(1) > div > input';
      await page.waitForSelector(startSelector);

      await page.evaluate(
        (selector, value) => {
          const input = document.querySelector(selector);
          if (input) {
            input.value = value;
            input.dispatchEvent(new Event('input', { bubbles: true }));
            input.dispatchEvent(new Event('change', { bubbles: true }));
          } else {
            console.error(`Elemen dengan selector ${selector} tidak ditemukan.`);
          }
        },
        startSelector,
        startdate
      );

      // enddate
      const enddate = enddate_long_format;
      const date = new Date(enddate_long_format);
      date.setDate(date.getDate() - 1);
      const enddate_yesterday = date.toISOString().split('T')[0]; // hasil: '2025-07-02'

      // console.log(enddate_yesterday);

      const endSelector = 'body > div > section > div > div:nth-child(3) > div > div > div > form > div > div:nth-child(2) > div > input';
      await page.waitForSelector(endSelector);

      await page.evaluate(
        (selector, value) => {
          const input = document.querySelector(selector);
          if (input) {
            input.value = value;
            input.dispatchEvent(new Event('input', { bubbles: true }));
            input.dispatchEvent(new Event('change', { bubbles: true }));
          } else {
            console.error(`Elemen dengan selector ${selector} tidak ditemukan.`);
          }
        },
        endSelector,
        enddate_yesterday
      );
      const button = await page.$('body > div > section > div > div:nth-child(3) > div > div > div > form > div > div:nth-child(4) > div > button');
      if (button) {
        await Promise.all([button.click(), page.waitForNavigation({ waitUntil: 'networkidle0' }).catch((e) => void 0)]);
      }
      // console.log('tombol ter klik');
    } else {
      const startdate = enddate_long_format;
      const date = new Date(enddate_long_format);
      date.setDate(date.getDate() - 1);
      const startdate_yesterday = date.toISOString().split('T')[0]; // hasil: '2025-07-02'

      // console.log(startdate_yesterday);

      const startSelector = 'body > div > section > div > div > div > div:nth-child(1) > div > form > div > div:nth-child(1) > div > input';
      await page.waitForSelector(startSelector);

      await page.evaluate(
        (selector, value) => {
          const input = document.querySelector(selector);
          if (input) {
            input.value = value;
            input.dispatchEvent(new Event('input', { bubbles: true }));
            input.dispatchEvent(new Event('change', { bubbles: true }));
          } else {
            console.error(`Elemen dengan selector ${selector} tidak ditemukan.`);
          }
        },
        startSelector,
        startdate_yesterday
      );
      const button = await page.$('body > div > section > div > div > div > div:nth-child(1) > div > form > div > div:nth-child(3) > div > button');
      if (button) {
        await Promise.all([button.click(), page.waitForNavigation({ waitUntil: 'networkidle0' }).catch((e) => void 0)]);
      }
    }

    const button = await page.$('body > div > section > div > div > div > div:nth-child(1) > div > form > div > div:nth-child(3) > div > button');
    if (button) {
      await Promise.all([button.click(), page.waitForNavigation({ waitUntil: 'networkidle0' }).catch((e) => void 0)]);
    }

    // Ambil data tabel
    const data = await page.evaluate((selector) => {
      const table = document.querySelector(selector);
      if (!table) return '';

      const rows = Array.from(table.querySelectorAll('tr'));
      return rows
        .map((row) => {
          const columns = Array.from(row.querySelectorAll('td, th'));
          return columns.map((column) => column.innerText.trim().replace(/,/g, '.')).join(',');
        })
        .join('\n');
    }, selector);

    if (data) {
      fs.writeFileSync(`loaded_file/asr_wifi/${nameFile}.csv`, data);
      console.log(`${nameFile} berhasil didownload!`);
    } else {
      console.log(`Tabel dengan selector ${selector} tidak ditemukan!`);
    }
  }

  // Fungsi untuk mengambil data dashboard
  async function getDataFromDashboard(url, fileCSV) {
    await page.goto(url, { waitUntil: 'networkidle2' });

    if (url.includes('sugar')) {
      await getDataFromTable('body > div > section > div > div > div > div:nth-child(2) > div > table', `${fileCSV}_regional`);
      await getDataFromTable('body > div > section > div > div > div > div:nth-child(5) > div > table', `${fileCSV}_teritory`);
    } else {
      await getDataFromTable('body > div > section > div > div:nth-child(5) > div > div > div > div > table', `${fileCSV}_regional`);
      await getDataFromTable('body > div > section > div > div:nth-child(6) > div:nth-child(2) > div > div > table', `${fileCSV}_teritory`);
    }
  }

  await getDataFromDashboard('https://smarthub.wifi.id/website/index.php/report/sugar_2025', 'sugar');
  await getDataFromDashboard('https://smarthub.wifi.id/website/index.php/report/ttr_compliance_2025', 'ttr');
  await browser.close();
})();
