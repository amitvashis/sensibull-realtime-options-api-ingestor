const { spawn } = require('child_process');
const path = require('path');

const DEFAULT_INTERVAL_SECONDS = 60;

function parseCliArgs(argv) {
    const args = {};

    for (let i = 2; i < argv.length; i += 1) {
        const key = argv[i];
        const value = argv[i + 1];

        if (!String(key).startsWith('--')) {
            continue;
        }

        const name = key.slice(2);
        if (!value || String(value).startsWith('--')) {
            args[name] = 'true';
            continue;
        }

        args[name] = value;
        i += 1;
    }

    return args;
}

function parsePositiveNumber(rawValue, fallbackValue) {
    const value = Number(rawValue);
    return Number.isFinite(value) && value > 0 ? value : fallbackValue;
}

function parseNonNegativeNumber(rawValue, fallbackValue) {
    const value = Number(rawValue);
    return Number.isFinite(value) && value >= 0 ? value : fallbackValue;
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function spawnNodeProcess(scriptPath, args, extraEnv = {}) {
    return new Promise((resolve) => {
        const child = spawn(process.execPath, [scriptPath, ...args], {
            stdio: 'inherit',
            env: { ...process.env, ...extraEnv },
            cwd: process.cwd()
        });

        child.on('close', (code) => resolve(code ?? 1));
        child.on('error', () => resolve(1));
    });
}

async function run() {
    const rawArgs = process.argv.slice(2);
    const parsed = parseCliArgs(process.argv);

    const schedulerKeys = new Set([
        '--interval-seconds',
        '--max-runs',
        '--stale-stop-runs',
        '--to-csv',
        '--csv-input',
        '--csv-output'
    ]);

    const forwardArgs = [];
    for (let i = 0; i < rawArgs.length; i += 1) {
        const token = rawArgs[i];
        if (!schedulerKeys.has(token)) {
            forwardArgs.push(token);
            continue;
        }

        if (token === '--to-csv') {
            continue;
        }

        i += 1;
    }

    const intervalSeconds = parsePositiveNumber(parsed['interval-seconds'], DEFAULT_INTERVAL_SECONDS);
    const maxRuns = parsed['max-runs'] ? parsePositiveNumber(parsed['max-runs'], null) : null;
    const staleStopRuns = parseNonNegativeNumber(parsed['stale-stop-runs'], 3);
    const toCsv = String(parsed['to-csv'] || '').toLowerCase() === 'true';
    const csvInput = parsed['csv-input'] || 'scrapped_data';
    const csvOutput = parsed['csv-output'] || 'csv_data/options_chain_all.csv';
    const runSummaryPath = path.resolve(process.cwd(), '.last-run-summary.json');

    const mainScript = path.resolve(process.cwd(), 'main.js');
    const csvScript = path.resolve(process.cwd(), 'convert-to-csv.js');

    let shouldStop = false;
    let runCount = 0;
    let staleRunCount = 0;

    process.on('SIGINT', () => {
        shouldStop = true;
    });
    process.on('SIGTERM', () => {
        shouldStop = true;
    });

    console.log(`Historical collector started. Interval: ${intervalSeconds}s`);
    console.log(`Forwarding args to main.js: ${forwardArgs.join(' ') || '(none)'}`);
    console.log(`Auto-stop on stale runs: ${staleStopRuns} consecutive run(s) with no new data`);
    if (toCsv) {
        console.log(`CSV refresh enabled: input=${csvInput} output=${csvOutput}`);
    }

    while (!shouldStop) {
        runCount += 1;
        const startedAt = new Date();
        const runTimestampUtc = startedAt.toISOString();

        console.log(`\n[Run ${runCount}] started at ${runTimestampUtc}`);
        const exitCode = await spawnNodeProcess(mainScript, forwardArgs, {
            SENSIBULL_RUN_TIMESTAMP_UTC: runTimestampUtc,
            SENSIBULL_RUN_SUMMARY_PATH: runSummaryPath
        });

        let runSummary = null;
        try {
            runSummary = JSON.parse(require('fs').readFileSync(runSummaryPath, 'utf8'));
        } catch (error) {
            runSummary = null;
        }

        if (exitCode !== 0) {
            console.error(`[Run ${runCount}] extractor failed with exit code ${exitCode}`);
        } else {
            console.log(`[Run ${runCount}] extractor completed`);
        }

        const appendedSnapshots = Number(runSummary?.appended_snapshots || 0);
        const duplicateSnapshots = Number(runSummary?.duplicate_snapshots || 0);
        if (exitCode === 0 && appendedSnapshots === 0 && duplicateSnapshots > 0) {
            staleRunCount += 1;
            console.log(`[Run ${runCount}] no new data appended (duplicate snapshots: ${duplicateSnapshots})`);
        } else if (exitCode === 0) {
            staleRunCount = 0;
        }

        if (staleStopRuns > 0 && staleRunCount >= staleStopRuns) {
            console.log(`No new data for ${staleRunCount} consecutive run(s). Stopping collector.`);
            break;
        }

        if (toCsv) {
            if (appendedSnapshots > 0 || !runSummary) {
                const csvCode = await spawnNodeProcess(csvScript, ['--input', csvInput, '--output', csvOutput]);
                if (csvCode !== 0) {
                    console.error(`[Run ${runCount}] CSV conversion failed with exit code ${csvCode}`);
                } else {
                    console.log(`[Run ${runCount}] CSV conversion completed`);
                }
            } else {
                console.log(`[Run ${runCount}] CSV conversion skipped (no new snapshots)`);
            }
        }

        if (maxRuns && runCount >= maxRuns) {
            break;
        }

        if (shouldStop) {
            break;
        }

        await sleep(intervalSeconds * 1000);
    }

    console.log('Historical collector stopped.');
}

run().catch((error) => {
    console.error(`Collector fatal error: ${error.message}`);
    process.exit(1);
});
