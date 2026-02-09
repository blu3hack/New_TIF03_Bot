const puppeteer = require('puppeteer');
const fs = require('fs');
const csv = require('csv-parser');
const { secureHeapUsed } = require('crypto');
const { user_aribi, pass_aribi } = require('./login');
const { exec } = require('child_process');
const axios = require('axios');
const sharp = require('sharp');
const TelegramBot = require('node-telegram-bot-api');
const mysql = require('mysql2/promise');
// const connection = require('./db_connection');
const { periode_long_format } = require('./currentDate');

(async () => {
  const browser = await puppeteer.launch({ headless: false }); // Buka browser (non-headless)
  const page = await browser.newPage();

  // ambil capcute
  await page.goto('https://nonatero.telkom.co.id/wsa/index.php/login');

  // Isi formulir login
  await page.type('[placeholder="Enter your Nik or username"]', user_aribi);
  await page.type('[placeholder="············"]', pass_aribi);
  page.$x("//a[contains(., 'Wallboard')]");

  // klik chec box
  const checkboxSelector = '#ck-terms-of-use';
  const isChecked = await page.$eval(checkboxSelector, (checkbox) => checkbox.checked);

  // Jika belum dicentang, klik untuk mencentangnya
  if (!isChecked) {
    await page.click(checkboxSelector);
    console.log('Checkbox berhasil diklik!');
  } else {
    console.log('Checkbox sudah dicentang.');
  }
  // Klik tombol login
  await page.click('#formAuthentication > button');

  // ================== INPUT OTP =====================
  async function getCaptchaFromDatabase() {
    return new Promise((resolve, reject) => {
      exec('python otp_840168.py', (error, stdout, stderr) => {
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
    const code_otp = await getCaptchaFromDatabase();
    const code1 = Math.floor(code_otp / 100000) % 10;
    const code2 = Math.floor(code_otp / 10000) % 10;
    const code3 = Math.floor(code_otp / 1000) % 10;
    const code4 = Math.floor(code_otp / 100) % 10;
    const code5 = Math.floor(code_otp / 10) % 10;
    const code6 = code_otp % 10;

    console.log(code1, code2, code3, code4, code5, code6);

    await page.type('#gg_otp > div > input.sscrt.scrt-1', code1.toString());
    await page.waitForTimeout(50);

    await page.type('#gg_otp > div > input.sscrt.scrt-2', code2.toString());
    await page.waitForTimeout(50);

    await page.type('#gg_otp > div > input.sscrt.scrt-3', code3.toString());
    await page.waitForTimeout(50);

    await page.type('#gg_otp > div > input.sscrt.scrt-4', code4.toString());
    await page.waitForTimeout(50);

    await page.type('#gg_otp > div > input.sscrt.scrt-5', code5.toString());
    await page.waitForTimeout(50);

    await page.type('#gg_otp > div > input.sscrt.scrt-6', code6.toString());
    await page.waitForTimeout(50);

    // Klik tombol login
    await page.click('#gg_otp > input.btn.btn-primary.mb-3');
    await page.waitForNavigation();
  }

  await insertOTP();
  await page.waitForTimeout(20000);

  async function goToLinkByXPath(xpath) {
    await page.waitForXPath(xpath);
    const links = await page.$x(xpath);
    if (links.length > 0) {
      const linkUrl = await page.evaluate((link) => link.href, links[0]);
      await page.goto(linkUrl, { waitUntil: 'networkidle2', timeout: 5000 }).catch((e) => void 0);
    }
  }

  async function segmen_gamas_akses() {
    await goToLinkByXPath("//a[contains(., 'Segmen Gamas Akses')]");
  }

  async function ttr36() {
    await goToLinkByXPath("//a[. = 'TTR']");
  }

  async function service_a() {
    await goToLinkByXPath("//a[contains(., 'Service Availability')]");
  }
  async function sugar() {
    await goToLinkByXPath("//a[contains(., 'Assurance Guarantee')]");
  }

  async function ttr3_diamond() {
    await goToLinkByXPath("//a[. = 'TTR']");
  }

  async function file_download() {
    await goToLinkByXPath("//a[contains(., 'File Downlod')]");
  }

  // Ambil data WSA GAMAS
  async function SegmenAkses() {
    console.log('============== SEGMEN GAMAS AKSES ===============');
    await segmen_gamas_akses();

    async function sugar_assurance(company, regional, nama_file) {
      // pilih company
      await page.waitForSelector('#company');
      await page.click('#company');

      await page.evaluate((company) => {
        const optionElement = document.querySelector(`#company > option:nth-child(${company})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, company);
      await page.waitForTimeout(3000);

      // pilih regional
      await page.waitForSelector('#regional');
      await page.click('#regional');

      await page.evaluate((regional) => {
        const optionElement = document.querySelector(`#regional > option:nth-child(${regional})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, regional);
      await page.waitForTimeout(3000);

      // pilih tanggal periode
      const result = periode_long_format;
      // console.log(result);

      const inputSelector = '#periode_1_'; // Ganti dengan selector elemen input Anda
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

      await tombol();
      const segment_wsa = await page.evaluate(() => {
        const table = document.querySelector(
          'body > div.layout-wrapper.layout-navbar-full.layout-horizontal.layout-without-menu > div > div > div > div.container-xxl.flex-grow-1.container-p-y > div > div > div.nav-align-top.mb-4 > table:nth-child(2)',
        );
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
      // console.log(segment_wsa);
      fs.writeFileSync(`loaded_file/wsa_gamas/${nama_file}.csv`, segment_wsa);
      console.log(`${nama_file} berhasil didownload \n`);
    }

    await sugar_assurance(5, 1, 'segmen_tif');
    await sugar_assurance(5, 4, 'segmen_district');
    await sugar_assurance(3, 1, 'segmen_reg');
    await sugar_assurance(3, 5, 'segmen_tr4');
    await sugar_assurance(3, 6, 'segmen_tr5');
  }

  // Pengambilan data WSA Assurance
  //   ================== proses pengambilan data table =========================
  async function AsrGuarantee() {
    console.log('============== Assurance Guarantee WSA ===============');
    await page.waitForTimeout(10000);
    // await sugar();

    async function sugar_assurance(company, regional, witel, nama_file) {
      // pilih tahun periode
      const periode_selector = '#periodeValue';
      const option_periode_Selector = '#periodeValue > option:nth-child(1)';
      await page.waitForSelector(periode_selector);
      await page.evaluate(
        (periode_selector, option_periode_Selector) => {
          const selectElement = document.querySelector(periode_selector);
          const optionElement = document.querySelector(option_periode_Selector);
          if (selectElement && optionElement) {
            selectElement.value = optionElement.value;
            const event = new Event('change', { bubbles: true });
            selectElement.dispatchEvent(event);
          }
        },
        periode_selector,
        option_periode_Selector,
      );

      // pilih company
      await page.waitForSelector('#company');
      await page.click('#company');

      await page.evaluate((company) => {
        const optionElement = document.querySelector(`#company > option:nth-child(${company})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, company);
      await page.waitForTimeout(3000);

      // pilih regional
      await page.waitForSelector('#regional');
      await page.click('#regional');

      await page.evaluate((regional) => {
        const optionElement = document.querySelector(`#regional > option:nth-child(${regional})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, regional);
      await page.waitForTimeout(3000);

      // pilih witel
      await page.waitForSelector('#witel');
      await page.click('#witel');

      await page.evaluate((witel) => {
        const optionElement = document.querySelector(`#witel > option:nth-child(${witel})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, witel);
      await page.waitForTimeout(10000);

      await tombol();
      const sugar_wsa = await page.evaluate(() => {
        const table = document.querySelector('#navs-teknis > table:nth-child(4)');
        const rows = Array.from(table.querySelectorAll('tr'));
        return rows
          .map((row) => {
            const columns = Array.from(row.querySelectorAll('td, th'));
            return columns.map((column) => column.innerText).join(',');
          })
          .join('\n');
      });
      // console.log(sugar_wsa);
      fs.writeFileSync(`loaded_file/wsa_gamas/${nama_file}.csv`, sugar_wsa);
      console.log(`${nama_file} berhasil didownload \n`);
    }

    await sugar_assurance(5, 1, 1, 'sugar_tif');
    await sugar_assurance(5, 2, 1, 'sugar_district_tif1');
    await sugar_assurance(5, 3, 1, 'sugar_district_tif2');
    await sugar_assurance(5, 4, 1, 'sugar_district_tif3');
    await sugar_assurance(5, 5, 1, 'sugar_district_tif4');
    await sugar_assurance(3, 1, 1, 'sugar_nas');
    await sugar_assurance(3, 2, 1, 'sugar_tr1');
    await sugar_assurance(3, 3, 1, 'sugar_tr2');
    await sugar_assurance(3, 4, 1, 'sugar_tr3');
    await sugar_assurance(3, 5, 1, 'sugar_tr4');
    await sugar_assurance(3, 6, 1, 'sugar_tr5');
    await sugar_assurance(3, 7, 1, 'sugar_tr6');
    await sugar_assurance(3, 8, 1, 'sugar_tr7');

    await sugar_assurance(2, 4, 1, 'sugar_area');
    await sugar_assurance(2, 4, 2, 'sugar_balnus');
    await sugar_assurance(2, 4, 3, 'sugar_jateng');
    await sugar_assurance(2, 4, 4, 'sugar_jatim');

    await sugar_assurance(1, 4, 1, 'sugar_area_ccm');
    await sugar_assurance(1, 4, 2, 'sugar_balnus_ccm');
    await sugar_assurance(1, 4, 3, 'sugar_jateng_ccm');
    await sugar_assurance(1, 4, 4, 'sugar_jatim_ccm');
  }

  // =============

  async function ServAvailability() {
    console.log('============== Service Availibility ===============');
    await page.waitForTimeout(10000);
    // await service_a();

    async function ser_avail(company, regional, witel, nama_file) {
      // pilih tahun periode
      const periode_selector = '#periodeValue';
      const option_periode_Selector = '#periodeValue > option:nth-child(1)';
      await page.waitForSelector(periode_selector);
      await page.evaluate(
        (periode_selector, option_periode_Selector) => {
          const selectElement = document.querySelector(periode_selector);
          const optionElement = document.querySelector(option_periode_Selector);
          if (selectElement && optionElement) {
            selectElement.value = optionElement.value;
            const event = new Event('change', { bubbles: true });
            selectElement.dispatchEvent(event);
          }
        },
        periode_selector,
        option_periode_Selector,
      );

      // pilih company
      await page.waitForSelector('#company');
      await page.click('#company');

      await page.evaluate((company) => {
        const optionElement = document.querySelector(`#company > option:nth-child(${company})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, company);
      await page.waitForTimeout(3000);

      // pilih regional
      await page.waitForSelector('#regional');
      await page.click('#regional');

      await page.evaluate((regional) => {
        const optionElement = document.querySelector(`#regional > option:nth-child(${regional})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, regional);
      await page.waitForTimeout(3000);

      // pilih witel
      await page.waitForSelector('#witel');
      await page.click('#witel');

      await page.evaluate((witel) => {
        const optionElement = document.querySelector(`#witel > option:nth-child(${witel})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, witel);
      await page.waitForTimeout(3000);

      // klik tombol
      await tombol();

      const service_availibility = await page.evaluate(() => {
        const table = document.querySelector('#navs-teknis > table:nth-child(4)');
        const rows = Array.from(table.querySelectorAll('tr'));
        return rows
          .map((row) => {
            const columns = Array.from(row.querySelectorAll('td, th'));
            return columns.map((column) => column.innerText).join(',');
          })
          .join('\n');
      });
      console.log(service_availibility);
      fs.writeFileSync(`loaded_file/wsa_gamas/${nama_file}.csv`, service_availibility);
      console.log(`${nama_file} berhasil didownload \n`);
    }

    await ser_avail(5, 1, 1, 'service_tif');
    await ser_avail(5, 2, 1, 'service_district_tif1');
    await ser_avail(5, 3, 1, 'service_district_tif2');
    await ser_avail(5, 4, 1, 'service_district_tif3');
    await ser_avail(5, 5, 1, 'service_district_tif4');
    await ser_avail(3, 1, 1, 'service_nas');
    await ser_avail(3, 2, 1, 'service_tr1');
    await ser_avail(3, 3, 1, 'service_tr2');
    await ser_avail(3, 4, 1, 'service_tr3');
    await ser_avail(3, 5, 1, 'service_tr4');
    await ser_avail(3, 6, 1, 'service_tr5');
    await ser_avail(3, 7, 1, 'service_tr6');
    await ser_avail(3, 8, 1, 'service_tr7');

    await ser_avail(2, 4, 1, 'service_area');
    await ser_avail(2, 4, 2, 'service_balnus');
    await ser_avail(2, 4, 3, 'service_jateng');
    await ser_avail(2, 4, 4, 'service_jatim');

    await ser_avail(1, 4, 1, 'service_area_ccm');
    await ser_avail(1, 4, 2, 'service_balnus_ccm');
    await ser_avail(1, 4, 3, 'service_jateng_ccm');
    await ser_avail(1, 4, 4, 'service_jatim_ccm');
  }
  //  ================ TTR ===============

  async function ttr() {
    console.log('============== TTR COMPLIANCE ===============');
    await page.waitForTimeout(10000);
    // await ttr3_diamond();

    async function jenis_ttr(indexType, kategori_ttr) {
      await page.waitForTimeout(10000);
      await page.waitForSelector('#ttr_type');
      await page.click('#ttr_type');

      await page.evaluate((indexType) => {
        const optionElement = document.querySelector(`#ttr_type > option:nth-child(${indexType})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, indexType);
      await page.waitForTimeout(3000);

      async function sub_ttr(company, regional, witel, nama_file) {
        // pilih tahun periode
        const periode_selector = '#periodeValue';
        const option_periode_Selector = '#periodeValue > option:nth-child(1)';
        await page.waitForSelector(periode_selector);
        await page.evaluate(
          (periode_selector, option_periode_Selector) => {
            const selectElement = document.querySelector(periode_selector);
            const optionElement = document.querySelector(option_periode_Selector);
            if (selectElement && optionElement) {
              selectElement.value = optionElement.value;
              const event = new Event('change', { bubbles: true });
              selectElement.dispatchEvent(event);
            }
          },
          periode_selector,
          option_periode_Selector,
        );

        // pilih company
        await page.waitForSelector('#company');
        await page.click('#company');

        await page.evaluate((company) => {
          const optionElement = document.querySelector(`#company > option:nth-child(${company})`);
          if (optionElement) {
            optionElement.selected = true;
            const selectElement = optionElement.parentElement;
            selectElement.dispatchEvent(new Event('change'));
          } else {
            console.error('Elemen <option> tidak ditemukan.');
          }
        }, company);
        await page.waitForTimeout(3000);

        await page.waitForSelector('#regional');
        await page.click('#regional');

        await page.evaluate((regional) => {
          const optionElement = document.querySelector(`#regional > option:nth-child(${regional})`);
          if (optionElement) {
            optionElement.selected = true;
            const selectElement = optionElement.parentElement;
            selectElement.dispatchEvent(new Event('change'));
          } else {
            console.error('Elemen <option> tidak ditemukan.');
          }
        }, regional);
        await page.waitForTimeout(3000);

        // pilih witel
        await page.waitForSelector('#witel');
        await page.click('#witel');

        await page.evaluate((witel) => {
          const optionElement = document.querySelector(`#witel > option:nth-child(${witel})`);
          if (optionElement) {
            optionElement.selected = true;
            const selectElement = optionElement.parentElement;
            selectElement.dispatchEvent(new Event('change'));
          } else {
            console.error('Elemen <option> tidak ditemukan.');
          }
        }, witel);
        await page.waitForTimeout(3000);

        await tombol();
        const ttr_diamond = await page.evaluate(() => {
          const table = document.querySelector('#navs-1_teknis > table:nth-child(4)');
          const rows = Array.from(table.querySelectorAll('tr'));
          return rows
            .map((row) => {
              const columns = Array.from(row.querySelectorAll('td, th'));
              return columns.map((column) => column.innerText).join(',');
            })
            .join('\n');
        });
        // console.log(ttr_diamond);
        fs.writeFileSync(`loaded_file/wsa_gamas/${kategori_ttr}_${nama_file}.csv`, ttr_diamond);
        console.log(`${kategori_ttr}_${nama_file} berhasil didownload \n`);
      }

      await sub_ttr(5, 1, 1, 'tif');
      await sub_ttr(5, 2, 1, 'district_tif1');
      await sub_ttr(5, 3, 1, 'district_tif2');
      await sub_ttr(5, 4, 1, 'district_tif3');
      await sub_ttr(5, 5, 1, 'district_tif4');
      await sub_ttr(3, 1, 1, 'nas');
      await sub_ttr(3, 2, 1, 'tr1');
      await sub_ttr(3, 3, 1, 'tr2');
      await sub_ttr(3, 4, 1, 'tr3');
      await sub_ttr(3, 5, 1, 'tr4');
      await sub_ttr(3, 6, 1, 'tr5');
      await sub_ttr(3, 7, 1, 'tr6');
      await sub_ttr(3, 8, 1, 'tr7');

      await sub_ttr(2, 4, 1, 'area');
      await sub_ttr(2, 4, 2, 'balnus');
      await sub_ttr(2, 4, 3, 'jateng');
      await sub_ttr(2, 4, 4, 'jatim');

      await sub_ttr(1, 4, 1, 'area_ccm');
      await sub_ttr(1, 4, 2, 'balnus_ccm');
      await sub_ttr(1, 4, 3, 'jateng_ccm');
      await sub_ttr(1, 4, 4, 'jatim_ccm');
    }

    await jenis_ttr(1, 'ttr3');
    await jenis_ttr(2, 'ttr6');
    await jenis_ttr(4, 'ttr36');
    await jenis_ttr(5, 'ttrmanja');
  }

  // Proses DOwnload Data
  await ttr();
  await SegmenAkses();
  await AsrGuarantee();
  await ServAvailability();

  async function tombol() {
    const [button] = await page.$x("//button[contains(., 'Filter')]");
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
  await browser.close();
})();
