// loadfile.js
const { spawn } = require('child_process');

function runCommand(cmd, args = [], label = '', delay = 0) {
  return new Promise((resolve) => {
    if (label) console.log(label);

    const child = spawn(cmd, args, {
      cwd: process.cwd(),
      stdio: 'inherit',
      shell: true,
    });

    child.on('close', (code) => {
      console.log(`✅ Proses ${[cmd, ...args].join(' ')} selesai (exit code: ${code})`);
      if (delay > 0) {
        console.log(`⏳ Menunggu ${delay / 1000} detik...`);
        setTimeout(resolve, delay);
      } else {
        resolve();
      }
    });
  });
}

async function main() {
  console.log('Melakukan Proses Pengolahan data MTTR MSO');
  await new Promise((r) => setTimeout(r, 5000)); // delay 5 detik
  await runCommand('node', ['simarvel'], '\n========= Proses Download data si marvel =========', 5000);
  await runCommand('node', ['simarvel_loaddata'], '\n========= Proses load data simarvel =========', 5000);
  await runCommand('node', ['simarvel_input'], '\n========= Proses input data simarvel =========', 5000);
  await new Promise((r) => setTimeout(r, 3000)); // delay 3 detik
}

main();
