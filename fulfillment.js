const puppeteer = require('puppeteer');
const connection = require('./connection');
// const connection = require('./db');
const { user_care, pass_care } = require('./login');
const { periode_short_format, startdate_short_format, enddate_short_format } = require('./currentDate');
const { get } = require('request-promise-native');

(async () => {
  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();

  try {
    // Navigasi ke halaman login
    await page.goto('https://dashboard.telkom.co.id/infrastructure');

    // Screenshoot cpt
    const element = await page.$('#captcha-element > img'); // ganti #main dengan id target
    if (element) {
      await element.screenshot({
        path: 'captcha/cpt.png',
      });
    } else {
      console.log('? Elemen dengan id tersebut tidak ditemukan');
    }

    // // mbil cpacha dari database
    function getData() {
      return new Promise((resolve, reject) => {
        const query = "SELECT pesan FROM get_otp_for_download WHERE pesan LIKE '%cpt%' ORDER BY id DESC LIMIT 1";
        connection.query(query, (err, results) => {
          if (err) return reject(err);
          resolve(results);
        });
      });
    }

    // Isi formulir login
    await page.waitForSelector('#uname');
    await page.type('#uname', user_care);
    await page.type('#passw', pass_care);
    await page.waitForTimeout(10000);
    await page.waitForSelector('#captcha-input');
    const result = await getData();
    const pesan = result[0].pesan; // contoh: "cpt azp"
    const parts = pesan.split(' ');
    const captcha = parts[1] || null; // ambil kata setelah "cpt"
    console.log(captcha);
    await page.type('#captcha-input', String(captcha)); // pastikan string

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
    await page.waitForTimeout(3000);

    // Klik tombol login
    await page.click('#submit');

    // Menunggu OTP dimasukkan ke dalam database
    console.log('Mengambil OTP dari database...');
    await page.waitForTimeout(5000);

    const getOtp = () => {
      return new Promise((resolve, reject) => {
        const query = 'SELECT otp FROM bot_message WHERE username_sender = "DashboardVerificationBot" ORDER BY created_at DESC LIMIT 1';
        connection.query(query, (error, results) => {
          if (error) {
            return reject(error);
          }
          if (!results.length || !results[0].otp) {
            return reject(new Error('Tidak ada OTP di database.'));
          }
          resolve(results[0].otp);
        });
      });
    };

    try {
      const otp = await getOtp();
      console.log('OTP yang didapat:', otp);

      await page.waitForSelector('input[name="verifikasi"]');
      await page.type('input[name="verifikasi"]', otp);
      await page.click('body > div.content > form > div.form-actions > button');
    } catch (error) {
      console.error('Gagal mengambil OTP:', error.message);
    } finally {
      connection.end(); // Tutup koneksi database
    }

    // Fungsi untuk mengambil data dari halaman
    console.log('Logged in successfully!');
    const url = 'https://dashboard.telkom.co.id/fulfillment';
    await page.goto(url, {
      waitUntil: ['networkidle0', 'domcontentloaded'],
      timeout: 0,
    });

    async function wsa_fulfillment_ih(page) {
      const fs = require('fs');
      const path = require('path');
      const source = 'C:/Users/L/Downloads/';
      const destination = 'D:/SCRAPPERS/Scrapper/loaded_file/download_fulfillment';
      // Mulai proses lainnya setelah login
      console.log('sudah masuk >> wsa fulfillment Indihome');
      await page.waitForTimeout(3000);

      // Tunggu submenu muncul dan klik pada "KPI Wecare 2024" menggunakan XPath

      async function fulfillment_data(fileName) {
        // await page.evaluate((index) => {
        //   const elements = Array.from(document.querySelectorAll('span'));
        //   const matchingElements = elements.filter((el) => el.textContent.includes('KPI Endstate Monthly'));

        //   console.log(`Jumlah elemen yang mengandung "KPI Endstate Monthly":`, matchingElements.length);

        //   if (matchingElements.length > index) {
        //     matchingElements[index].click(); // Klik elemen sesuai urutan yang diinginkan
        //   } else {
        //     console.log(`Elemen dengan urutan ke-${index + 1} tidak ditemukan.`);
        //   }
        // }, index); // Ganti angka ini dengan indeks elemen yang ingin diklik (misalnya 0, 1, 2, dst)

        const selectSelector = '#periode';
        const optionValue = periode_short_format;
        await page.waitForSelector(selectSelector);
        await page.evaluate(
          (selectSelector, optionValue) => {
            const selectElement = document.querySelector(selectSelector);
            if (selectElement) {
              selectElement.value = optionValue;
              const event = new Event('change', { bubbles: true });
              selectElement.dispatchEvent(event);
            }
          },
          selectSelector,
          optionValue,
        );
        await page.waitForTimeout(10000);
        // Tunggu elemen tombol siap
        await page.waitForSelector('#form > div > div > div.form-group.col-md-1 > button');

        // Klik tombol
        // await page.click('#form > div > div > div.form-group.col-md-1 > button');

        async function ambil_data_wsa_ff() {
          const selector_reg = ['#table_lokasi'];

          for (i = 0; i < selector_reg.length; i++) {
            await page.waitForSelector(selector_reg[i]);
            await page.click(selector_reg[i]);
          }

          // ambil data dari table
          await page.waitForSelector('#table_lokasi', { timeout: 0 });
          const wsa_ful = await page.evaluate(() => {
            const table = document.querySelector('#table_lokasi > tbody');
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
          fs.writeFileSync(`loaded_file/wsa/${fileName}.csv`, wsa_ful);
          console.log(`${fileName} berhasil didownload \n`);
        }
        await ambil_data_wsa_ff();
      }

      async function get_download(treg) {
        const selector = `#table_lokasi > tbody > tr:nth-child(${treg}) > td:nth-child(15) > a`;
        await page.waitForSelector(selector, { visible: true });
        await page.click(selector);
        console.log(`File regionan ${treg + 1} berhasil didownload`);
      }

      async function move_fulfillment_reg() {
        // Pastikan folder tujuan ada
        if (!fs.existsSync(destination)) {
          fs.mkdirSync(destination, { recursive: true });
        }

        fs.readdir(source, (err, files) => {
          if (err) return console.error('Gagal membaca folder:', err);

          files.forEach((file) => {
            if (file.includes('data_detail_') && (file.endsWith('.xlsx') || file.endsWith('.xls'))) {
              const oldPath = path.join(source, file);
              const newPath = path.join(destination, file);

              // Copy file
              const readStream = fs.createReadStream(oldPath);
              const writeStream = fs.createWriteStream(newPath);

              readStream.pipe(writeStream);

              writeStream.on('finish', () => {
                // Hapus file asal setelah berhasil dicopy
                fs.unlink(oldPath, (err) => {
                  if (err) {
                    console.error(`Gagal menghapus file asal ${file}:`, err);
                  } else {
                    console.log(`Berhasil memindahkan file: ${file}`);
                  }
                });
              });

              writeStream.on('error', (err) => {
                console.error(`Gagal menulis file ${file}:`, err);
              });
            }
          });
        });
      }

      // await page.waitForTimeout(60000);
      // await get_download(1);
      // await get_download(402);
      // await get_download(580);
      // await get_download(721);
      // await get_download(890);
      // await get_download(1217);
      // await get_download(1372);
      // await fulfillment_data('wsa_fulfillment_reg');

      // // await fulfillment_data(1, 'wsa_fulfillment_reg');
      // // await fulfillment_data(10, 'wsa_fulfillment_tif');
      console.log('Lakukan Selection manual pada KPI TIF Endstate Monthly');

      await page.waitForTimeout(60000);
      await get_download(1);
      await get_download(397);
      await get_download(708);
      await get_download(1191);
      await fulfillment_data('wsa_fulfillment_tif');
      console.log('Proses Pemindahan FIle Download ke Folder Project');
      await page.waitForTimeout(20000);
      await move_fulfillment_reg();
      console.log('Proses Pengambilan WSA Fulfillment Ih Selesai');
    }

    async function wsa_fulfillment_ccm(page) {
      console.log('Lakukan Selection manual pada KPI Fulfillment CCM Endstate Monthly');
      const fs = require('fs');
      const path = require('path');
      const source = 'C:/Users/L/Downloads/';
      const destination = 'D:/SCRAPPERS/Scrapper/loaded_file/download_fulfillment';
      // Mulai proses lainnya setelah login
      console.log('sudah masuk >> wsa fulfillment Indihome');
      await page.waitForTimeout(3000);

      // Tunggu submenu muncul dan klik pada "KPI Wecare 2024" menggunakan XPath

      async function fulfillment_data(fileName) {
        const selectSelector = '#periode';
        const optionValue = periode_short_format;
        await page.waitForSelector(selectSelector);
        await page.evaluate(
          (selectSelector, optionValue) => {
            const selectElement = document.querySelector(selectSelector);
            if (selectElement) {
              selectElement.value = optionValue;
              const event = new Event('change', { bubbles: true });
              selectElement.dispatchEvent(event);
            }
          },
          selectSelector,
          optionValue,
        );
        await page.waitForTimeout(10000);
        // Tunggu elemen tombol siap
        await page.waitForSelector('#form > div > div > div.form-group.col-md-1 > button');

        // Klik tombol
        // await page.click('#form > div > div > div.form-group.col-md-1 > button');

        async function ambil_data_wsa_ff() {
          const selector_reg = ['#table3'];

          for (i = 0; i < selector_reg.length; i++) {
            await page.waitForSelector(selector_reg[i]);
            await page.click(selector_reg[i]);
          }

          // ambil data dari table
          await page.waitForSelector('#table3', { timeout: 0 });
          const wsa_ful = await page.evaluate(() => {
            const table = document.querySelector('#table3 > tbody');
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
          fs.writeFileSync(`loaded_file/wsa/${fileName}.csv`, wsa_ful);
          console.log(`${fileName} berhasil didownload \n`);
        }
        await ambil_data_wsa_ff();
      }

      await page.waitForTimeout(60000);

      await fulfillment_data('wsa_fulfillment_ccm');
      console.log('Proses Pengambilan WSA Fulfillment CCM Selesai');
    }

    const fs = require('fs');

    async function wsa_fulfillment_hsi(page) {
      console.log('sudah masuk >> wsa fulfillment HSI');
      await page.waitForTimeout(3000);

      async function fulfillment_data(index, fileName) {
        await page.evaluate((index) => {
          const elements = Array.from(document.querySelectorAll('span'));
          const matchingElements = elements.filter((el) => el.textContent.includes('KPI MSA EBIS'));
          if (matchingElements.length > index) {
            matchingElements[index].click();
          } else {
            console.log(`Elemen dengan urutan ke-${index + 1} tidak ditemukan.`);
          }
        }, index);

        const startdate = startdate_short_format;
        const enddate = enddate_short_format;

        console.log(startdate);
        const startSelector = '#startdate';
        await page.waitForSelector(startSelector);
        await page.evaluate(
          (selector, value) => {
            const input = document.querySelector(selector);
            if (input) {
              input.value = value;
              input.dispatchEvent(new Event('input', { bubbles: true }));
              input.dispatchEvent(new Event('change', { bubbles: true }));
            }
          },
          startSelector,
          startdate,
        );

        console.log(enddate);
        const endSelector = '#enddate';
        await page.waitForSelector(endSelector);
        await page.evaluate(
          (selector, value) => {
            const input = document.querySelector(selector);
            if (input) {
              input.value = value;
              input.dispatchEvent(new Event('input', { bubbles: true }));
              input.dispatchEvent(new Event('change', { bubbles: true }));
            }
          },
          endSelector,
          enddate,
        );

        await page.waitForTimeout(3000);

        console.log('Select menu');
        const selectSelector = '#SEGMEN';

        await page.waitForSelector(selectSelector);
        const optionValues = await page.$$eval(`${selectSelector} option`, (options) => options.map((option) => option.value));
        await page.select(selectSelector, ...optionValues);
        console.log('Semua option terpilih:', optionValues);

        await page.waitForTimeout(10000);
        console.log('Klik tombol submit...');
        await Promise.all([page.waitForSelector('#table_lokasi > tbody tr', { timeout: 0 }), page.click('#form > div > div > div.form-group.col-md-1 > button')]);
        console.log('Tombol submit diklik, data tabel mulai muncul');

        async function ambil_data_wsa_ff() {
          const tableSelector = '#table_lokasi';

          // Tunggu sampai tabel muncul
          await page.waitForSelector(tableSelector, { timeout: 0 }); // max 2 menit

          // Tunggu sampai tabel memiliki baris data
          await page.waitForFunction(
            () => {
              const table = document.querySelector('#table_lokasi > tbody');
              return table && table.querySelectorAll('tr').length > 0;
            },
            { timeout: 0 },
          );

          const wsa_ful = await page.evaluate(() => {
            const table = document.querySelector('#table_lokasi > tbody');
            const rows = Array.from(table.querySelectorAll('tr'));
            return rows
              .map((row) => {
                const columns = Array.from(row.querySelectorAll('td, th'));
                return columns
                  .map((column) => {
                    let cellValue = column.innerText.trim();
                    return cellValue.replace(/,/g, '');
                  })
                  .join(',');
              })
              .join('\n');
          });

          fs.writeFileSync(`loaded_file/wsa/${fileName}.csv`, wsa_ful);
          console.log(`${fileName} berhasil didownload\n`);
        }

        await page.waitForTimeout(20000);
        await ambil_data_wsa_ff();
      }

      await fulfillment_data(0, 'wsa_fulfillment_hsi_reg');
      await fulfillment_data(1, 'wsa_fulfillment_hsi_tif');
      console.log('Proses Pengambilan WSA Fulfillment HSI Selesai');
    }

    await wsa_fulfillment_ih(page);
    // await wsa_fulfillment_hsi(page);
    // await wsa_fulfillment_ccm(page);
  } catch (error) {
    console.error('Terjadi kesalahan:', error.message);
  } finally {
    await browser.close();
  }
})();
