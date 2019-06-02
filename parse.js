/**

NOTE: graceful-fs is used to avoide errors like EMFILE: too many open files

node parse.js \
  --sources="/some/path/a.csv.gz /some/path/b.csv.gz /some/path/c.csv.gz /some/path/d.csv.gz" \
  --destination="/some/path/data/parsed/intervals" \
  --weekdays="0,1,2,3,4,5,6" \
  --batch=625 \
  --sourceInterval=5 \
  --targetInterval=60

 */

const argv = require('yargs').argv;
const process = require('process');
const { once } = require('events');
// const fs = require('fs');
const fs = require('graceful-fs');
const os = require('os');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');
const moment = require('moment');

let totalLinesParsed = 0;

function updateProgress(progress){
  process.stdout.clearLine();
  process.stdout.cursorTo(0);
  process.stdout.write(`Parsed lines: ${progress}`);
}

function write(obj, destination, targetInterval) {
  try {
    for (const key in obj) {
      const entry = `${obj[key].join(os.EOL)}${os.EOL}`;

      const name = moment()
        .startOf('week')
        .add(key * targetInterval, 'minutes')
        .format('dddd-HHmm')
        .toLowerCase();

      fs.appendFile(path.join(...[destination, `${name}.csv`]), entry, (error) => {
        // if (error) throw error;
        if (error) console.log(error);
      });
    }

    updateProgress(totalLinesParsed);
  } catch (error) {
    throw error;
  }
}

async function parse(options) {
  try {
    const {
      source,
      destination,
      batch,
      intervals,
      weekdays,
    } = options;

    const fileStream = fs
      .createReadStream(source)
      .pipe(zlib.createGunzip());

    const rl = readline
      .createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

    let cnt = 0;
    let obj = {};

    rl
      .on('line', (line) => {
        cnt += 1;
        totalLinesParsed += 1;

        const [start, end, ...speeds] = line.split(',');

        for (let i = 0; i < speeds.length; i += 1) {
          if ((i * intervals.source) % intervals.target !== 0) continue;

          const weekday = Math.floor(i / (speeds.length / 7));
          if (!weekdays[weekday]) continue;

          const entry = `${start},${end},${speeds[i]}`;
          const key = i / (intervals.target / intervals.source);
          if (obj[key]) {
            obj[key].push(entry);
          } else {
            obj[key] = [entry];
          }
        }

        if (batch === 0 || cnt % batch === 0) {
          write(obj, destination, intervals.target)
          obj = {};
        }
      })
      .on('close', () => {
        write(obj, destination, intervals.target)
      });

    await once(rl, 'close');
  } catch (error) {
    throw error;
  }
}

(async () => {
  try {
    const {
      sources,
      destination,
      batch = 625,
      sourceInterval = 5,
      targetInterval = 5,
      weekdays = '0,1,2,3,4,5,6',
    } = argv;

    if (!sources) throw new Error('missing --sources');
    if (!destination) throw new Error('missing --destination');
    if (targetInterval < sourceInterval) throw new Error('--targetInterval cannot be less than --sourceInterval');
    if (targetInterval % sourceInterval !== 0) throw new Error('--targetInterval and --sourceInterval must be multiples of each other');

    const start = Date.now();
    const _sources = sources.split(' ');

    const _weekdays = weekdays
      .toString()
      .split(',')
      .map((weekday) => weekday.trim())
      .reduce((red, weekday) => ({...red, [weekday]: true }), {});

    await fs.promises.mkdir(destination, { recursive: true });

    console.log(`Destination directory: ${destination}`);

    updateProgress(0);
    for (const source of _sources) {
      await parse({
        source,
        destination,
        batch,
        weekdays: _weekdays,
        intervals: {
          source: sourceInterval,
          target: targetInterval,
        },
      });
    }

    process.stdout.write(`${os.EOL}Finished in ${Date.now() - start}ms${os.EOL}`);
  } catch (error) {
    console.log(error);
  }
})();
