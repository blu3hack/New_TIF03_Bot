const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const { startdate_short_format, enddate_short_format, periode_short_format } = require('./currentDate');

const COOKIE_PATH = path.join(__dirname, 'session.json');
const OUTPUT_DIR = path.join(__dirname, 'loaded_file/wifi_revi');

(async () => {
  try {
    const browser = await puppeteer.launch({ headless: false });
    const page = await browser.newPage();

    // Cek apakah ada session sebelumnya
    if (fs.existsSync(COOKIE_PATH)) {
      const cookies = JSON.parse(fs.readFileSync(COOKIE_PATH, 'utf-8'));
      await page.setCookie(...cookies);
      console.log('Cookies loaded, skipping login.');
    }

    await page.goto('https://dashboard.telkom.co.id/idwifi', { waitUntil: 'networkidle2' });

    // Jika tidak ada cookies, lakukan login
    if (!fs.existsSync(COOKIE_PATH)) {
      await page.type('#uname', '54028');
      await page.type('#passw', 'telkom135');

      console.log('Masukkan Captcha secara Manual dan tunggu sebentar');
      await page.waitForTimeout(10000);

      // Cek dan klik checkbox jika belum dicentang
      const checkboxSelector = '#agree';
      if (await page.$(checkboxSelector)) {
        const isChecked = await page.$eval(checkboxSelector, (checkbox) => checkbox.checked);
        if (!isChecked) {
          await page.click(checkboxSelector);
          console.log('Checkbox berhasil diklik!');
        } else {
          console.log('Checkbox sudah dicentang.');
        }
      }
      await page.click('#submit');

      console.log('Mengambil OTP dari database...');
      await page.waitForTimeout(20000);
      await page.click('body > div.content > form > div.form-actions > button');

      // Simpan cookies
      const cookies = await page.cookies();
      fs.writeFileSync(COOKIE_PATH, JSON.stringify(cookies, null, 2));
      console.log('Cookies saved.');
    }

    // Lanjutkan dengan aksi setelah login
    console.log('Logged in successfully!');

    async function wifi_revi() {
      console.log('Pilih Menu Relokasi');
      await page.waitForSelector('span', { timeout: 10000 }); // Tunggu elemen span muncul

      const clicked = await page.evaluate(() => {
        const elements = Array.from(document.querySelectorAll('span'));
        const matchingElements = elements.filter((el) => el.textContent.includes('Relokasi'));

        if (matchingElements.length > 0) {
          matchingElements[0].click(); // Klik elemen pertama yang ditemukan
          return true;
        }
        return false;
      });

      if (clicked) {
        console.log('Klik pada elemen Relokasi berhasil!');
      } else {
        console.log('Elemen Relokasi tidak ditemukan.');
      }

      // Tunggu halaman selesai loading setelah klik menu Relokasi
      await page.waitForTimeout(5000);

      // select date
      const result = periode_short_format;
      console.log(result);

      const inputSelector = '#startdate'; // Ganti dengan selector elemen input Anda
      await page.waitForSelector(inputSelector);

      await page.evaluate(
        (selector, value) => {
          const input = document.querySelector(selector);
          if (input) {
            input.value = value; // Ubah nilai
            input.dispatchEvent(new Event('input', { bubbles: true })); // Trigger event input
            input.dispatchEvent(new Event('change', { bubbles: true })); // Trigger event change
          } else {
            console.error(`Elemen dengan selector ${selector} tidak ditemukan.`);
          }
        },
        inputSelector,
        result,
      );

      async function getTable(fokus, fileName) {
        // Pilih Fokus
        const hrefValue = fokus;
        const selector = `a[href="${hrefValue}"]`;

        try {
          // Tunggu elemen muncul sebelum mengklik
          await page.waitForSelector(selector, { timeout: 0 });
          // Klik tombol
          await page.click(selector);
          console.log(`Tombol dengan href="${hrefValue}" berhasil diklik!`);
        } catch (error) {
          console.error(`Gagal menemukan atau mengklik tombol dengan href="${hrefValue}":`, error);
        }

        // Ambil Data dari table
        await page.waitForTimeout(10000);
        console.log('Mengambil data dari tabel...');
        await page.waitForSelector('#table1 > tbody', { timeout: 0 });

        const wifi_revi = await page.evaluate(() => {
          const table = document.querySelector('#table1 > tbody');
          if (!table) return '';

          const rows = Array.from(table.querySelectorAll('tr'));
          return rows
            .map((row) => {
              const columns = Array.from(row.querySelectorAll('td, th'));
              return columns
                .map((column) => {
                  let cellValue = column.innerText.trim();
                  return cellValue.replace(/,/g, '.'); // Hilangkan koma
                })
                .join(',');
            })
            .join('\n');
        });

        if (!wifi_revi) {
          console.log('Tabel kosong atau tidak ditemukan.');
        } else {
          fs.writeFileSync(path.join(OUTPUT_DIR, `${fileName}.csv`), wifi_revi);
          console.log(`${fileName}.csv berhasil disimpan`);
        }
      }

      await getTable('#ftelkom', 'wifi_revi_tif');
      await getTable('#ftif', 'wifi_revi_reg');
    }

    async function av_wifi(regional, kategori, fileName) {
      await page.waitForSelector('span', { timeout: 10000 }); // Tunggu elemen span muncul

      const clicked = await page.evaluate(() => {
        const elements = Array.from(document.querySelectorAll('span'));
        const matchingElements = elements.filter((el) => el.textContent.includes('Report Availability'));

        if (matchingElements.length > 0) {
          matchingElements[0].click(); // Klik elemen pertama yang ditemukan
          return true;
        }
        return false;
      });

      if (clicked) {
        console.log();
      } else {
        console.log('Elemen Report Availability tidak ditemukan.');
      }

      // Tunggu halaman selesai loading setelah klik menu Report Availability
      await page.waitForTimeout(5000);

      // pilih regional
      await page.waitForSelector('#divre');
      await page.click('#divre');

      await page.evaluate((regional) => {
        const optionElement = document.querySelector(`#divre > option:nth-child(${regional})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, regional);
      await page.waitForTimeout(3000);

      // pilih kategori
      await page.waitForSelector('#kategori');
      await page.click('#kategori');

      await page.evaluate((kategori) => {
        const optionElement = document.querySelector(`#kategori > option:nth-child(${kategori})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, kategori);
      await page.waitForTimeout(3000);

      // pilih startdate
      const startdate = startdate_short_format;
      console.log(startdate);

      const inputSelector = '#startdate'; // Ganti dengan selector elemen input Anda
      await page.waitForSelector(inputSelector);

      // Ubah nilai input menjadi "2024-12-01 to 2024-12-31"
      await page.evaluate(
        (selector, value) => {
          const input = document.querySelector(selector);
          if (input) {
            input.value = value; // Ubah nilai
            input.dispatchEvent(new Event('input', { bubbles: true })); // Trigger event input
            input.dispatchEvent(new Event('change', { bubbles: true })); // Trigger event change
          } else {
            console.error(`Elemen dengan selector ${selector} tidak ditemukan.`);
          }
        },
        inputSelector,
        startdate,
      );

      // pilih enddate
      const enddate = enddate_short_format;
      console.log(enddate);

      const endSelector = '#enddate'; // Ganti dengan selector elemen input Anda
      await page.waitForSelector(endSelector);

      await page.evaluate(
        (selector, value) => {
          const input = document.querySelector(selector);
          if (input) {
            input.value = value; // Ubah nilai
            input.dispatchEvent(new Event('input', { bubbles: true })); // Trigger event input
            input.dispatchEvent(new Event('change', { bubbles: true })); // Trigger event change
          } else {
            console.error(`Elemen dengan selector ${selector} tidak ditemukan.`);
          }
        },
        endSelector,
        enddate,
      );

      await tombol();

      // ambil data dari table
      await page.waitForSelector('#table_detil > tbody', { timeout: 0 });

      const av_wifi = await page.evaluate(() => {
        const table = document.querySelector('#table_detil > tbody');
        if (!table) return '';

        const rows = Array.from(table.querySelectorAll('tr'));
        return rows
          .map((row) => {
            const columns = Array.from(row.querySelectorAll('td, th'));
            return columns
              .map((column) => {
                let cellValue = column.innerText.trim();
                return cellValue.replace(/,/g, '.'); // Hilangkan koma
              })
              .join(',');
          })
          .join('\n');
      });

      if (!av_wifi) {
        console.log('Tabel kosong atau tidak ditemukan.');
      } else {
        fs.writeFileSync(path.join(OUTPUT_DIR, `${fileName}.csv`), av_wifi);
        console.log(`${fileName}.csv berhasil disimpan`);
      }
    }

    await wifi_revi();

    // await av_wifi(1, 1, 'av_all_reg');
    // await av_wifi(1, 2, 'av_ms_reg');
    // await av_wifi(1, 3, 'av_basic_reg');

    // await av_wifi(5, 1, 'av_all_tr4');
    // await av_wifi(5, 2, 'av_ms_tr4');
    // await av_wifi(5, 3, 'av_basic_tr4');

    // await av_wifi(6, 1, 'av_all_tr5');
    // await av_wifi(6, 2, 'av_ms_tr5');
    // await av_wifi(6, 3, 'av_basic_tr5');

    // tombol
    async function tombol() {
      const [button] = await page.$x('//*[@id="form"]/div/div/div[6]/div/button');
      if (button) {
        await Promise.all([
          button.click(),
          page
            .waitForNavigation({
              waitUntil: 'networkidle0',
            })
            .catch((e) => void 0),
        ]);
      }
    }

    // Pilih Menu Relokasi

    // Tutup browser
    await browser.close();
  } catch (error) {
    console.error('Terjadi kesalahan:', error);
  }
})();
