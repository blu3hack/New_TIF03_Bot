const puppeteer = require('puppeteer');
const pool = require('./connection');
const { user, pass } = require('./login');
const fs = require('fs');
const { periode_long_format, enddate_long_format, periode_end_format } = require('./currentDate');
const { exec } = require('child_process');

(async () => {
  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();

  try {
    await page.goto('https://newxpro.telkom.co.id/ebis');

    // Screenshoot captcha // Screenshoot captcha
    const [element] = await page.$x('/html/body/div[1]/div[3]/div/div[2]/div[2]/form/div[4]/div/img');
    if (element) {
      await element.screenshot({ path: 'captcha/cpt.png' });
    } else {
      console.log('❌ Elemen dengan XPath tersebut tidak ditemukan');
    }

    // mbil cpacha dari database
    function getData() {
      return new Promise((resolve, reject) => {
        const query = "SELECT pesan FROM get_otp_for_download WHERE pesan LIKE '%cpt%' ORDER BY id DESC LIMIT 1";
        pool.query(query, (err, results) => {
          if (err) return reject(err);
          resolve(results);
        });
      });
    }

    // Login
    await page.waitForSelector('#username');
    await page.type('#username', user);
    await page.type('#password', pass);

    await page.waitForTimeout(20000);
    const result = await getData();
    const pesan = result[0].pesan; // contoh: "cpt azp"
    const parts = pesan.split(' ');
    const captcha = parts[1] || null; // ambil kata setelah "cpt"
    console.log(captcha);
    await page.type('#captcha_val', String(captcha)); // pastikan string

    const checkboxSelector = '[name="terms"]';
    if (await page.$(checkboxSelector)) {
      const isChecked = await page.$eval(checkboxSelector, (el) => el.checked);
      if (!isChecked) await page.click(checkboxSelector);
    }

    await Promise.all([page.waitForNavigation({ waitUntil: 'networkidle0' }), page.click('form button[type="submit"]')]);
    console.log('Masukkan OTP secara Manual dan tunggu sebentar');

    // ================== INPUT OTP =====================
    async function getCaptchaFromPython() {
      return new Promise((resolve, reject) => {
        exec('python otp_newexpro.py', (error, stdout, stderr) => {
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

    // input otp secara manual
    await page.waitForTimeout(10000);
    async function insertOTP() {
      const result = await getCaptchaFromPython();
      // const pesan = result[0].pesan; // contoh: "cpt azp"
      // const parts = pesan.split(' ');
      // const captcha = parts[1] || null; // ambil kata setelah "cpt"
      console.log(result);
      const code_otp = result;
      const code1 = Math.floor(code_otp / 100000) % 10;
      const code2 = Math.floor(code_otp / 10000) % 10;
      const code3 = Math.floor(code_otp / 1000) % 10;
      const code4 = Math.floor(code_otp / 100) % 10;
      const code5 = Math.floor(code_otp / 10) % 10;
      const code6 = code_otp % 10;

      console.log(code1, code2, code3, code4, code5, code6);

      await page.type('#formZ_ > div.mb-6 > div > input:nth-child(1)', code1.toString());
      await page.waitForTimeout(50);

      await page.type('#formZ_ > div.mb-6 > div > input:nth-child(2)', code2.toString());
      await page.waitForTimeout(50);

      await page.type('#formZ_ > div.mb-6 > div > input:nth-child(3)', code3.toString());
      await page.waitForTimeout(50);

      await page.type('#formZ_ > div.mb-6 > div > input:nth-child(4)', code4.toString());
      await page.waitForTimeout(50);

      await page.type('#formZ_ > div.mb-6 > div > input:nth-child(5)', code5.toString());
      await page.waitForTimeout(50);

      await page.type('#formZ_ > div.mb-6 > div > input:nth-child(6)', code6.toString());
      await page.waitForTimeout(50);

      // Klik tombol login
      await page.click('#aktivasiButton');
      await page.waitForNavigation();
    }

    await insertOTP();

    await page.waitForTimeout(10000);
    console.log('Berhasil login ke NewXPro');

    // Ambil data ttr ffg

    async function ttdc_non_hsi(territory, regional, fileName) {
      await page.waitForTimeout(3000);
      console.log('Mengambil data TTR FFG');
      await page.goto('https://newxpro.telkom.co.id/ebis/msa/ttdc-2025', { waitUntil: 'networkidle0' });

      // function pengambilan data ttdc non hsi
      async function getDataTable(territory, regional, fileName) {
        const inputs = await page.$$('input[data-te-select-input-ref]');

        await inputs[0].click(); // index mulai dari 0 → ini klik elemen ke-2
        const [span_territory] = await page.$x(`//span[contains(., '${territory}')]`);
        if (span_territory) {
          await span_territory.click();
        } else {
          console.log(`Elemen span dengan teks ${territory} tidak ditemukan.`);
        }

        await inputs[1].click(); // index mulai dari 0 → ini klik elemen ke-2
        const [span_regional] = await page.$x(`//span[contains(., '${regional}')]`);
        if (span_regional) {
          await span_regional.click();
        } else {
          console.log(`Elemen span dengan teks ${regional} tidak ditemukan.`);
        }

        if (fileName.includes('wifi')) {
          await inputs[2].click(); // klik elemen ke-2
          const [span_jenis] = await page.$x(`//span[contains(., 'WIFI')]`);
          if (span_jenis) {
            await span_jenis.click();
          } else {
            console.log(`Elemen span dengan teks WIFI tidak ditemukan.`);
          }
        }
        // pilih tanggal
        // console.log(result);

        async function selectdate(selectorDate, result) {
          const inputSelector = selectorDate; // Ganti dengan selector elemen input Anda
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
            result
          );
        }

        await selectdate('#provcomp_date', periode_long_format);
        await selectdate('#order_date', periode_end_format);

        await page.waitForTimeout(3000);
        await Promise.all([page.waitForNavigation({ waitUntil: 'networkidle0' }), page.click('#filter_data > div > div:nth-child(8) > div > div > a')]);

        // ambil data dari table
        await page.waitForSelector('#LoadingImage', { hidden: true });
        const ttd_non_hsi = await page.evaluate(() => {
          const tbody = document.querySelector('#orderNONJT > div.flex.flex-col > div > div > table');
          if (!tbody) return 'tbody #table2 tidak ditemukan';

          const rows = Array.from(tbody.querySelectorAll('tr'));
          return rows
            .map((row) => {
              const columns = Array.from(row.querySelectorAll('td, th'));
              return columns.map((c) => c.textContent.trim()).join(',');
            })
            .filter((line) => line.trim() !== '') // buang baris kosong
            .join('\n');
        });

        // console.log(ttd_non_hsi);
        fs.writeFileSync(`loaded_file/ff_non_hsi/${fileName}.csv`, ttd_non_hsi);
        console.log(`${fileName} berhasil didownload`);
      }
      await getDataTable('NEW TREG', 'ALL', 'ttdc_tif');
      await getDataTable('NEW TREG', 'REGIONAL 3', 'ttdc_district');
      await getDataTable('OLD TREG', 'ALL', 'ttdc_reg');
      await getDataTable('OLD TREG', 'REGIONAL 4', 'ttdc_reg4');
      await getDataTable('OLD TREG', 'REGIONAL 5', 'ttdc_reg5');

      // ttdc wifi
      await getDataTable('NEW TREG', 'ALL', 'ttdc_wifi_tif');
      await getDataTable('NEW TREG', 'REGIONAL 3', 'ttdc_wifi_district');
      await getDataTable('OLD TREG', 'ALL', 'ttdc_wifi_reg');
      await getDataTable('OLD TREG', 'REGIONAL 4', 'ttdc_wifi_reg4');
      await getDataTable('OLD TREG', 'REGIONAL 5', 'ttdc_wifi_reg5');
    }

    async function ffg_non_hsi() {
      await page.waitForTimeout(3000);
      console.log('Mengambil data FFG');
      await page.goto('https://newxpro.telkom.co.id/ebis/msa/ffg-datin', {
        waitUntil: 'domcontentloaded',
        timeout: 120000, // 2 menit
      });

      // function pengambilan data ffg non hsi
      async function getdata(territory, regional, fileName) {
        const inputs = await page.$$('input[data-te-select-input-ref]');
        await inputs[0].click(); // index mulai dari 0 → ini klik elemen ke-2
        const [span_territory] = await page.$x(`//span[contains(., '${territory}')]`);

        if (span_territory) {
          await span_territory.click();
        } else {
          console.log(`Elemen span dengan teks ${territory} tidak ditemukan.`);
        }

        await inputs[1].click(); // index mulai dari 0 → ini klik elemen ke-2
        const [span_regional] = await page.$x(`//span[contains(., '${regional}')]`);

        if (span_regional) {
          await span_regional.click();
        } else {
          console.log(`Elemen span dengan teks ${regional} tidak ditemukan.`);
        }

        const result = enddate_long_format;
        // console.log(result);

        async function selectdate(selectorDate) {
          const inputSelector = selectorDate; // Ganti dengan selector elemen input Anda
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
            result
          );
        }

        await selectdate('#orderdate');

        await page.waitForTimeout(3000);
        await Promise.all([page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }), page.click('#filter_data > div > div:nth-child(6) > div > div > a')]);

        // ambil data dari table
        // tunggu sampai ada isi data (selain header)
        async function getDataTable(table, fileData) {
          // tunggu sampai tabel benar-benar terisi
          await page.waitForFunction(
            (selector) => {
              const tbody = document.querySelector(selector);
              if (!tbody) return false;

              const rows = tbody.querySelectorAll('tr');
              if (rows.length < 2) return false; // header + minimal 1 data

              return Array.from(rows).some((r) => Array.from(r.querySelectorAll('td')).some((td) => td.textContent.trim() !== ''));
            },
            { timeout: 0 },
            table // lempar variabel ke dalam fungsi di atas
          );

          // ambil data tabel
          const ttd_non_hsi = await page.evaluate((selector) => {
            const tbody = document.querySelector(selector);
            if (!tbody) return `tbody ${selector} tidak ditemukan`;

            const rows = Array.from(tbody.querySelectorAll('tr'));
            return rows
              .map((row) => {
                const columns = Array.from(row.querySelectorAll('td, th'));
                return columns.map((c) => c.textContent.trim()).join(',');
              })
              .filter((line) => line.trim() !== '')
              .join('\n');
          }, table);

          const header = 'witel,avg,jml,real';
          const csvContent = `${header}\n${ttd_non_hsi}`;

          // console.log(csvContent);
          fs.writeFileSync(`loaded_file/ff_non_hsi/${fileData}.csv`, csvContent);
          console.log(`${fileData} berhasil didownload`);
        }

        await getDataTable('table #table1', `ffg_${fileName}`);
        await getDataTable('table #table2', `ttr_ffg_${fileName}`);
      }
      await getdata('NEW TREG', 'ALL', 'tif');
      await getdata('NEW TREG', 'REGIONAL 3', 'district');
      await getdata('OLD TREG', 'ALL', 'reg');
      await getdata('OLD TREG', 'REGIONAL 4', 'reg4');
      await getdata('OLD TREG', 'REGIONAL 5', 'reg5');
    }

    async function provcomp() {
      await page.waitForTimeout(3000);
      await page.goto('https://newxpro.telkom.co.id/ebis/msa/kpi-datin-2025', { waitUntil: 'networkidle0' });

      // function pengambilan data provcomp
      async function getDataTable(territory, regional, fileName) {
        // selector
        const inputs = await page.$$('input[data-te-select-input-ref]');

        await inputs[0].click(); // index mulai dari 0 → ini klik elemen ke-2
        const [span_territory] = await page.$x(`//span[contains(., '${territory}')]`);
        if (span_territory) {
          await span_territory.click();
        } else {
          console.log(`Elemen span dengan teks ${territory} tidak ditemukan.`);
        }

        await inputs[1].click(); // index mulai dari 0 → ini klik elemen ke-2
        const [span_regional] = await page.$x(`//span[contains(., '${regional}')]`);
        if (span_regional) {
          await span_regional.click();
        } else {
          console.log(`Elemen span dengan teks ${regional} tidak ditemukan.`);
        }

        if (fileName.includes('wifi')) {
          await inputs[5].click(); // klik elemen ke-2
          const [span_jenis] = await page.$x(`//span[contains(., 'WIFI')]`);
          if (span_jenis) {
            await span_jenis.click();
          } else {
            console.log(`Elemen span dengan teks WIFI tidak ditemukan.`);
          }
        }
        // pilih tanggal
        // const result = periode_long_format;
        // console.log(result);

        async function selectdate(selectorDate, result) {
          const inputSelector = selectorDate; // Ganti dengan selector elemen input Anda
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
            result
          );
        }

        await selectdate('#orderdate', periode_long_format);
        await selectdate('#provcomp', periode_end_format);

        await page.waitForTimeout(3000);
        await Promise.all([page.waitForNavigation({ waitUntil: 'networkidle0' }), page.click('#filter_data > div > div[class~="w-[100px]"] > div > div > a')]);

        // ambil data dari table
        await page.waitForSelector('#LoadingImage', { hidden: true });
        const ttd_non_hsi = await page.evaluate(() => {
          const tbody = document.querySelector('#table1');
          if (!tbody) return 'tbody #table2 tidak ditemukan';

          const rows = Array.from(tbody.querySelectorAll('tr'));
          return rows
            .map((row) => {
              const columns = Array.from(row.querySelectorAll('td, th'));
              return columns.map((c) => c.textContent.trim()).join(',');
            })
            .filter((line) => line.trim() !== '') // buang baris kosong
            .join('\n');
        });

        // console.log(ttd_non_hsi);
        fs.writeFileSync(`loaded_file/ff_non_hsi/${fileName}.csv`, ttd_non_hsi);
        console.log(`${fileName} berhasil didownload`);
      }
      await getDataTable('NEW TREG', 'ALL', 'provcomp_tif');
      await getDataTable('NEW TREG', 'REGIONAL 3', 'provcomp_district');
      await getDataTable('OLD TREG', 'ALL', 'provcomp_reg');
      await getDataTable('OLD TREG', 'REGIONAL 4', 'provcomp_reg4');
      await getDataTable('OLD TREG', 'REGIONAL 5', 'provcomp_reg5');

      // provcomp wifi
      await getDataTable('NEW TREG', 'ALL', 'provcomp_wifi_tif');
      await getDataTable('NEW TREG', 'REGIONAL 3', 'provcomp_wifi_district');
      await getDataTable('OLD TREG', 'ALL', 'provcomp_wifi_reg');
      await getDataTable('OLD TREG', 'REGIONAL 4', 'provcomp_wifi_reg4');
      await getDataTable('OLD TREG', 'REGIONAL 5', 'provcomp_wifi_reg5');
    }

    async function unspec_warranty(territory, regional, fileName) {
      await page.waitForTimeout(3000);
      await page.goto('https://newxpro.telkom.co.id/ebis/msa/underspec-waranty', { waitUntil: 'networkidle0' });

      // Function pengambilan data unspec warranty

      async function getDataTable(territory, regional, fileName) {
        const inputs = await page.$$('input[data-te-select-input-ref]');
        await inputs[0].click(); // index mulai dari 0 → ini klik elemen ke-2
        const [span_territory] = await page.$x(`//span[contains(., '${territory}')]`);
        if (span_territory) {
          await span_territory.click();
        } else {
          console.log(`Elemen span dengan teks ${territory} tidak ditemukan.`);
        }

        await inputs[1].click(); // index mulai dari 0 → ini klik elemen ke-2
        const [span_regional] = await page.$x(`//span[contains(., '${regional}')]`);
        if (span_regional) {
          await span_regional.click();
        } else {
          console.log(`Elemen span dengan teks ${regional} tidak ditemukan.`);
        }

        if (fileName.includes('wifi')) {
          await inputs[2].click(); // klik elemen ke-2
          const [span_jenis] = await page.$x(`//span[contains(., 'WIFI')]`);
          if (span_jenis) {
            await span_jenis.click();
          } else {
            console.log(`Elemen span dengan teks WIFI tidak ditemukan.`);
          }
        }
        // pilih tanggal
        const result = periode_long_format;
        // console.log(result);

        async function selectdate(selectorDate) {
          const inputSelector = selectorDate; // Ganti dengan selector elemen input Anda
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
            result
          );
        }

        await selectdate('#periode_pengukuran');

        await page.waitForTimeout(3000);
        await Promise.all([page.waitForNavigation({ waitUntil: 'networkidle0' }), page.click('#filter_data > div > div[class~="w-[100px]"] > div > div > a')]);

        // ambil data dari table
        await page.waitForSelector('#LoadingImage', { hidden: true });
        const ttd_non_hsi = await page.evaluate(() => {
          const tbody = document.querySelector('#table1');
          if (!tbody) return 'tbody #table2 tidak ditemukan';

          const rows = Array.from(tbody.querySelectorAll('tr'));
          return rows
            .map((row) => {
              const columns = Array.from(row.querySelectorAll('td, th'));
              return columns.map((c) => c.textContent.trim()).join(',');
            })
            .filter((line) => line.trim() !== '') // buang baris kosong
            .join('\n');
        });

        // console.log(ttd_non_hsi);
        fs.writeFileSync(`loaded_file/ff_non_hsi/${fileName}.csv`, ttd_non_hsi);
        console.log(`${fileName} berhasil didownload`);
      }

      await getDataTable('NEW TREG', 'ALL', 'unspec_warranty_tif');
      await getDataTable('NEW TREG', 'REGIONAL 3', 'unspec_warranty_district');
      await getDataTable('OLD TREG', 'ALL', 'unspec_warranty_reg');
      await getDataTable('OLD TREG', 'REGIONAL 4', 'unspec_warranty_reg4');
      await getDataTable('OLD TREG', 'REGIONAL 5', 'unspec_warranty_reg5');
    }

    // running FUnction
    // await ttdc_non_hsi();
    // await ffg_non_hsi();
    // await provcomp();
    await unspec_warranty();
  } catch (err) {
    console.error('Ada kesalahan:', err.message);
  } finally {
    await page.waitForTimeout(5000);
    await browser.close();
  }
})();
