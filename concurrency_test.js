// Real concurrency test for the driver lock.
// Several OS threads (worker_threads) fight over the same 200 drivers at the same moment.
// Run:  node concurrency_test.js
const { Worker, isMainThread, workerData, parentPort } = require('worker_threads');
const os = require('os');

const DRIVERS = 200, THREADS = 8, TRIES = 5000; // per thread

if (isMainThread) {
  const run = mode => new Promise(resolve => {
    const state = new Int32Array(new SharedArrayBuffer(4 * DRIVERS)); // 0 = AVAILABLE, 1 = OFFERED
    const wins  = new Int32Array(new SharedArrayBuffer(4 * DRIVERS)); // how many threads believe they got driver i
    const t0 = process.hrtime.bigint();
    let done = 0, blocked = 0;
    for (let i = 0; i < THREADS; i++) {
      const w = new Worker(__filename, { workerData: { mode, state, wins } });
      w.on('message', b => { blocked += b; });
      w.on('exit', () => {
        if (++done === THREADS) {
          const ms = Number(process.hrtime.bigint() - t0) / 1e6;
          const got = Array.from(wins).filter(v => v > 0).length;
          const dbl = Array.from(wins).filter(v => v > 1).length;
          const extra = Array.from(wins).reduce((a, v) => a + Math.max(0, v - 1), 0);
          resolve({ mode, attempts: THREADS * TRIES, got, dbl, extra, blocked, ms });
        }
      });
    }
  });
  (async () => {
    console.log(`${THREADS} threads on ${os.cpus().length} CPU core(s), ${DRIVERS} drivers, ${THREADS * TRIES} claim attempts each run\n`);
    for (const m of ['naive', 'atomic']) {
      const r = await run(m);
      console.log(`${m === 'naive' ? 'NAIVE  (check, then set)       ' : 'ATOMIC (compare-and-swap, like the Redis Lua lock)'}`);
      console.log(`  drivers claimed: ${r.got}`);
      console.log(`  drivers given to MORE THAN ONE rider: ${r.dbl}  (${r.extra} extra assignments)`);
      console.log(`  attempts blocked: ${r.blocked}`);
      console.log(`  time: ${r.ms.toFixed(1)} ms, ${Math.round(r.attempts / (r.ms / 1000)).toLocaleString()} attempts per second\n`);
    }
  })();
} else {
  const { mode, state, wins } = workerData;
  let blocked = 0;
  for (let i = 0; i < TRIES; i++) {
    const d = Math.floor(Math.random() * DRIVERS);
    if (mode === 'atomic') {
      // one indivisible step: only one thread can flip AVAILABLE to OFFERED
      if (Atomics.compareExchange(state, d, 0, 1) === 0) Atomics.add(wins, d, 1); else blocked++;
    } else {
      // read the state, then write it later: another thread can slip in between
      if (Atomics.load(state, d) === 0) {
        for (let k = 0; k < 2000; k++) Math.sqrt(k); // stands in for the delay of a network round trip
        Atomics.store(state, d, 1);
        Atomics.add(wins, d, 1);
      } else blocked++;
    }
  }
  parentPort.postMessage(blocked);
}
