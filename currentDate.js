// // Helper: fungsi untuk ambil tanggal dalam zona waktu Asia/Jakarta (WIB)
// function getDatePartsInJakarta(date = new Date()) {
//   const formatter = new Intl.DateTimeFormat('en-CA', {
//     timeZone: 'Asia/Jakarta',
//     year: 'numeric',
//     month: '2-digit',
//     day: '2-digit',
//   });

//   const formattedDate = formatter.format(date); // format: YYYY-MM-DD
//   const [yyyy, mm, dd] = formattedDate.split('-');

//   return { yyyy, mm, dd, formattedDate };
// }

// // Tanggal saat ini (WIB)
// const currentDate = new Date();
// const { yyyy, mm, dd, formattedDate: today } = getDatePartsInJakarta(currentDate);

// // Tanggal awal bulan (WIB)
// const startOfMonth = new Date(currentDate.getFullYear(), currentDate.getMonth(), 1);
// const { formattedDate: startdate, dd: startDD } = getDatePartsInJakarta(startOfMonth);

// // Tanggal terakhir bulan ini (WIB)
// const year = currentDate.getFullYear();
// const month = currentDate.getMonth(); // 0 = Jan
// const lastDayOfMonth = new Date(year, month + 1, 0);
// const { formattedDate: lastDate } = getDatePartsInJakarta(lastDayOfMonth);

// // Format-format yang diminta
// const periode_long_format = `${startdate} to ${today}`;
// const periode_end_format = `2025-01-01 to ${today}`;
// const periode_short_format = `${yyyy}${mm}`;
// const startdate_short_format = `${yyyy}${mm}${startDD}`;
// const today_short_format = today.replace(/-/g, '');

// const startdate_long_format = startdate;
// const enddate_short_format = today_short_format;
// const enddate_long_format = today;
// const insertDate = today;

// // const insertDate = '2026-02-28';

// console.log(lastDate);

// module.exports = {
//   periode_long_format,
//   periode_end_format,
//   periode_short_format,
//   startdate_short_format,
//   enddate_short_format,
//   startdate_long_format,
//   enddate_long_format,
//   insertDate,
//   MaxDate: lastDate,
// };

const periode_long_format = `2026-03-01 to 2026-03-31`;
const periode_short_format = `202603`;

const startdate_short_format = `20260301`;
const enddate_short_format = `20260331`;

const startdate_long_format = `2026-03-01`;
const enddate_long_format = `2026-03-31`;

const insertDate = '2026-03-31';
const periode_end_format = '2026-03-01 to 2026-03-31';
const lastDate = '2026-02-31';

module.exports = {
  periode_long_format: periode_long_format,
  periode_short_format: periode_short_format,
  startdate_short_format: startdate_short_format,
  startdate_long_format: startdate_long_format,
  enddate_short_format: enddate_short_format,
  enddate_long_format: enddate_long_format,
  periode_end_format: periode_end_format,
  insertDate: insertDate,
  MaxDate: lastDate,
};
