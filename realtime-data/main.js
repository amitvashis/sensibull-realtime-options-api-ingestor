const WebSocket = require('ws');
const lib = require('./lib.js');

const API_PROFILE_URL = 'https://api.sensibull.com/v1/users/me?source=platform';
const WS_BASE = 'wss://wsrelay.sensibull.com';
const ORIGIN = 'https://web.sensibull.com';
const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36';
const DEFAULT_SYMBOLS = ['NIFTY'];
const DEFAULT_MONTHS_AHEAD = 3;
const DEFAULT_TIMEOUT_MS = 180000;
const DEFAULT_MAX_RECONNECTS = 8;

function parseCliArgs(argv) {
    const args = {};

    for (let i = 2; i < argv.length; i += 1) {
        const key = argv[i];
        const value = argv[i + 1];
        if (!key.startsWith('--')) {
            continue;
        }

        const argName = key.slice(2);
        if (!value || value.startsWith('--')) {
            args[argName] = 'true';
            continue;
        }

        args[argName] = value;
        i += 1;
    }

    return args;
}

function parseSymbols(rawSymbols) {
    if (!rawSymbols) {
        return DEFAULT_SYMBOLS;
    }

    return String(rawSymbols)
        .split(',')
        .map((value) => value.trim().toUpperCase())
        .filter(Boolean);
}

function parsePositiveNumber(rawValue, fallbackValue) {
    const value = Number(rawValue);
    return Number.isFinite(value) && value > 0 ? value : fallbackValue;
}

function parseOptionalNumber(rawValue) {
    if (rawValue === undefined || rawValue === null || rawValue === '') {
        return null;
    }

    const value = Number(rawValue);
    if (!Number.isFinite(value)) {
        return null;
    }

    return value;
}

function parseInputConfig(argv, env) {
    const cli = parseCliArgs(argv);
    const positional = argv.slice(2).filter((value) => !String(value).startsWith('--'));

    const symbols = parseSymbols(cli.symbols || env.SENSIBULL_SYMBOLS || positional[0]);
    const strikeMin = parseOptionalNumber(cli['strike-min'] || env.SENSIBULL_STRIKE_MIN || positional[1]);
    const strikeMax = parseOptionalNumber(cli['strike-max'] || env.SENSIBULL_STRIKE_MAX || positional[2]);
    const monthsAhead = parsePositiveNumber(cli.months || env.SENSIBULL_MONTHS_AHEAD || positional[3], DEFAULT_MONTHS_AHEAD);
    const timeoutMs = parsePositiveNumber(cli['timeout-ms'] || env.SENSIBULL_TIMEOUT_MS || positional[4], DEFAULT_TIMEOUT_MS);
    const maxReconnects = parsePositiveNumber(cli['max-reconnects'] || env.SENSIBULL_MAX_RECONNECTS || positional[5], DEFAULT_MAX_RECONNECTS);

    if (strikeMin !== null && strikeMax !== null && strikeMin > strikeMax) {
        throw new Error(`Invalid strike range: min (${strikeMin}) is greater than max (${strikeMax})`);
    }

    return {
        symbols,
        strikeMin,
        strikeMax,
        monthsAhead,
        timeoutMs,
        maxReconnects,
        runTimestampUtc: cli['run-timestamp-utc'] || env.SENSIBULL_RUN_TIMESTAMP_UTC || new Date().toISOString(),
        consumerType: cli['consumer-type'] || env.SENSIBULL_CONSUMER_TYPE || ''
    };
}

function getCookieHeader(headers) {
    let setCookies = [];

    if (typeof headers.getSetCookie === 'function') {
        setCookies = headers.getSetCookie();
    } else {
        const headerValue = headers.get('set-cookie');
        if (headerValue) {
            setCookies = [headerValue];
        }
    }

    return setCookies
        .map((cookie) => cookie.split(';', 1)[0].trim())
        .filter(Boolean)
        .join('; ');
}

function mapPlanToConsumerType(billingPlanId) {
    const plan = String(billingPlanId || '').toLowerCase();

    if (plan.includes('pro_plus') || plan.includes('proplus')) {
        return 'platform_pro_plus';
    }

    if (plan.includes('pro')) {
        return 'platform_pro';
    }

    return 'platform_no_plan';
}

function resolveTokens(symbols) {
    const symbolToToken = new Map();

    lib.instruments.forEach((instrument) => {
        if (!instrument || !instrument.is_underlying || !instrument.tradingsymbol) {
            return;
        }

        symbolToToken.set(instrument.tradingsymbol.toUpperCase(), instrument.instrument_token);
    });

    const resolved = [];
    const missing = [];

    symbols.forEach((symbol) => {
        const token = symbolToToken.get(symbol);
        if (!token) {
            missing.push(symbol);
            return;
        }

        resolved.push({ symbol, token });
    });

    return { resolved, missing };
}

function startOfUtcDay(date) {
    return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function parseExpiryDate(expiry) {
    if (typeof expiry !== 'string') {
        return null;
    }

    const parts = expiry.split('-').map((part) => Number(part));
    if (parts.length !== 3 || parts.some((part) => !Number.isFinite(part))) {
        return null;
    }

    return new Date(Date.UTC(parts[0], parts[1] - 1, parts[2]));
}

function selectExpiriesForHorizon(underlyingStats, token, monthsAhead) {
    const perExpiryMap = underlyingStats?.[token]?.per_expiry_map;
    if (!perExpiryMap) {
        return [];
    }

    const today = startOfUtcDay(new Date());
    const horizonEnd = new Date(today);
    horizonEnd.setUTCMonth(horizonEnd.getUTCMonth() + monthsAhead);

    return Object.keys(perExpiryMap)
        .filter((expiry) => {
            const expiryDate = parseExpiryDate(expiry);
            if (!expiryDate) {
                return false;
            }

            return expiryDate >= today && expiryDate <= horizonEnd;
        })
        .sort();
}

function pairKey(token, expiry) {
    return `${token}|${expiry}`;
}

function resolveOptionChainRoot(message) {
    const payload = message?.payload || {};
    const tokenKey = String(message?.token ?? payload.token ?? '');
    const expiryKey = String(message?.expiry ?? payload.expiry ?? '');
    const tokenNode = payload?.data?.[tokenKey] || payload?.data?.[Number(tokenKey)] || null;
    if (!tokenNode) {
        return null;
    }

    const expiryNode = tokenNode?.[expiryKey] || null;
    if (!expiryNode || typeof expiryNode !== 'object') {
        return null;
    }

    return { tokenKey, expiryKey, root: expiryNode };
}

function filterOptionChainByStrike(message, strikeMin, strikeMax) {
    const resolved = resolveOptionChainRoot(message);
    if (!resolved || !resolved.root.chain) {
        return { originalCount: 0, filteredCount: 0 };
    }

    const chain = resolved.root.chain;
    const originalEntries = Object.entries(chain);
    const filteredEntries = originalEntries.filter(([strikeKey, value]) => {
        const strike = Number(value?.strike ?? strikeKey);
        if (!Number.isFinite(strike)) {
            return false;
        }

        if (strikeMin !== null && strike < strikeMin) {
            return false;
        }

        if (strikeMax !== null && strike > strikeMax) {
            return false;
        }

        return true;
    });

    resolved.root.chain = Object.fromEntries(filteredEntries);

    return {
        originalCount: originalEntries.length,
        filteredCount: filteredEntries.length
    };
}

function subscribe(ws, payload) {
    ws.send(JSON.stringify(payload));
}

async function fetchSessionInfo() {
    const response = await fetch(API_PROFILE_URL, {
        headers: {
            'User-Agent': USER_AGENT,
            'Origin': ORIGIN
        }
    });

    if (!response.ok) {
        throw new Error(`Profile API failed with status ${response.status}`);
    }

    const body = await response.json();
    const brokerId = body?.data?.broker_details?.broker_id;
    const billingPlanId = body?.data?.subscription_plan?.billing_plan_id || 'free';
    const cookieHeader = getCookieHeader(response.headers);

    if (!Number.isFinite(brokerId)) {
        throw new Error('Could not resolve broker id from profile response');
    }

    if (!cookieHeader) {
        throw new Error('No session cookie found in profile response');
    }

    return { brokerId, billingPlanId, cookieHeader };
}

async function main() {
    const config = parseInputConfig(process.argv, process.env);
    const { brokerId, billingPlanId, cookieHeader } = await fetchSessionInfo();
    const consumerType = config.consumerType || mapPlanToConsumerType(billingPlanId);
    const wsUrl = `${WS_BASE}/broker/${brokerId}?consumerType=${consumerType}`;

    const { resolved, missing } = resolveTokens(config.symbols);
    if (!resolved.length) {
        throw new Error(`No valid symbols found. Requested: ${config.symbols.join(', ')}`);
    }

    if (missing.length) {
        console.warn(`Skipping unknown symbols: ${missing.join(', ')}`);
    }

    const tokenToSymbol = Object.fromEntries(resolved.map((item) => [String(item.token), item.symbol]));
    const tokens = resolved.map((item) => item.token);
    const expiryPlanByToken = new Map();
    const targetPairs = new Set();
    const subscribedPairs = new Set();
    const receivedPairs = new Set();

    let ws = null;
    let reconnectTimer = null;
    let reconnectCount = 0;
    let timeoutHandle = null;
    let finished = false;
    let intentionalClose = false;
    let appendedSnapshots = 0;
    let duplicateSnapshots = 0;
    let filteredOutSnapshots = 0;
    const runSummaryPath = process.env.SENSIBULL_RUN_SUMMARY_PATH || '';

    function remainingPairs() {
        return targetPairs.size - receivedPairs.size;
    }

    function writeRunSummary(success, reason) {
        if (!runSummaryPath) {
            return;
        }

        const summary = {
            success: !!success,
            reason: reason || '',
            run_timestamp_utc: config.runTimestampUtc,
            symbols: config.symbols,
            strike_min: config.strikeMin,
            strike_max: config.strikeMax,
            months_ahead: config.monthsAhead,
            target_snapshots: targetPairs.size,
            received_snapshots: receivedPairs.size,
            appended_snapshots: appendedSnapshots,
            duplicate_snapshots: duplicateSnapshots,
            filtered_out_snapshots: filteredOutSnapshots,
            summary_timestamp_utc: new Date().toISOString()
        };

        try {
            require('fs').writeFileSync(runSummaryPath, JSON.stringify(summary, null, 2), 'utf8');
        } catch (error) {
            console.error(`Could not write run summary: ${error.message}`);
        }
    }

    function finish(success, reason) {
        if (finished) {
            return;
        }

        finished = true;

        if (timeoutHandle) {
            clearTimeout(timeoutHandle);
            timeoutHandle = null;
        }

        if (reconnectTimer) {
            clearTimeout(reconnectTimer);
            reconnectTimer = null;
        }

        if (!success) {
            process.exitCode = 1;
        }

        writeRunSummary(success, reason);

        if (reason) {
            console.log(reason);
        }

        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
            intentionalClose = true;
            ws.close();
            return;
        }

        console.log(`Summary: received ${receivedPairs.size}/${targetPairs.size} option-chain snapshots`);
    }

    function buildTargetsFromUnderlyingStats(payload) {
        let addedTargets = 0;

        tokens.forEach((token) => {
            const key = String(token);
            if (expiryPlanByToken.has(key)) {
                return;
            }

            const expiries = selectExpiriesForHorizon(payload, token, config.monthsAhead);
            if (!expiries.length) {
                console.warn(`No expiries found for ${tokenToSymbol[key] || key} in next ${config.monthsAhead} month(s)`);
                expiryPlanByToken.set(key, []);
                return;
            }

            expiryPlanByToken.set(key, expiries);
            expiries.forEach((expiry) => {
                const keyPair = pairKey(token, expiry);
                if (!targetPairs.has(keyPair)) {
                    targetPairs.add(keyPair);
                    addedTargets += 1;
                }
            });
        });

        return addedTargets;
    }

    function subscribePendingPairs() {
        if (!ws || ws.readyState !== WebSocket.OPEN) {
            return;
        }

        targetPairs.forEach((keyPair) => {
            if (receivedPairs.has(keyPair) || subscribedPairs.has(keyPair)) {
                return;
            }

            const [tokenText, expiry] = keyPair.split('|');
            const token = Number(tokenText);
            subscribe(ws, {
                msgCommand: 'subscribe',
                dataSource: 'option-chain',
                brokerId: brokerId,
                tokens: [],
                underlyingExpiry: [{ underlying: token, expiry: expiry }],
                uniqueId: ''
            });

            subscribedPairs.add(keyPair);
            console.log(`Subscribed ${tokenToSymbol[tokenText] || tokenText} expiry ${expiry}`);
        });
    }

    function scheduleReconnect(closeCode, closeReason) {
        if (finished || intentionalClose) {
            return;
        }

        const pendingCount = remainingPairs();
        if (pendingCount <= 0 && targetPairs.size > 0) {
            finish(true, 'All targets collected.');
            return;
        }

        if (reconnectCount >= config.maxReconnects) {
            finish(false, `Max reconnects reached. Last close: ${closeCode} ${closeReason}`);
            return;
        }

        reconnectCount += 1;
        const delayMs = Math.min(1000 * reconnectCount, 5000);
        console.warn(`Socket closed (${closeCode}) ${closeReason}. Reconnecting in ${delayMs}ms... (${reconnectCount}/${config.maxReconnects})`);
        reconnectTimer = setTimeout(() => {
            reconnectTimer = null;
            connect();
        }, delayMs);
    }

    function connect() {
        if (finished) {
            return;
        }

        intentionalClose = false;
        ws = new WebSocket(wsUrl, {
            headers: {
                'User-Agent': USER_AGENT,
                'Origin': ORIGIN,
                'Cookie': cookieHeader
            }
        });

        ws.on('open', () => {
            subscribedPairs.clear();
            console.log(`Connected to ${wsUrl}`);
            console.log(`Symbols: ${resolved.map((item) => item.symbol).join(', ')}`);
            console.log(`Filters: strike_min=${config.strikeMin ?? 'NA'} strike_max=${config.strikeMax ?? 'NA'} months=${config.monthsAhead}`);

            subscribe(ws, {
                msgCommand: 'subscribe',
                dataSource: 'underlying-stats',
                brokerId: brokerId,
                tokens: tokens,
                underlyingExpiry: [],
                uniqueId: ''
            });

            // If we already know target expiries from earlier connection(s), resume immediately.
            subscribePendingPairs();
        });

        ws.on('message', (rawData) => {
            const packet = Buffer.from(rawData);
            if (packet.length === 1 && packet[0] === 0xfd) {
                ws.send(Buffer.from([0xfe]));
                return;
            }

            let message;
            try {
                message = lib.decodeData(packet);
            } catch (error) {
                console.error(`Decode error: ${error.message}`);
                return;
            }

            if (message.kind === 5) {
                const added = buildTargetsFromUnderlyingStats(message.payload);
                if (added > 0) {
                    console.log(`Planned ${targetPairs.size} option-chain snapshots`);
                }
                subscribePendingPairs();
                return;
            }

            if (message.kind !== 3) {
                return;
            }

            const tokenText = String(message.token);
            const expiryText = String(message.expiry);
            const keyPair = pairKey(tokenText, expiryText);

            if (targetPairs.size > 0 && !targetPairs.has(keyPair)) {
                return;
            }

            const { originalCount, filteredCount } = filterOptionChainByStrike(message, config.strikeMin, config.strikeMax);
            if (filteredCount > 0) {
                message.collection_timestamp_utc = config.runTimestampUtc;
                message.collection_received_utc = new Date().toISOString();
                const appended = lib.print(message);
                if (appended) {
                    appendedSnapshots += 1;
                    console.log(`Saved ${tokenToSymbol[tokenText] || tokenText} ${expiryText}: strikes ${filteredCount}/${originalCount}`);
                } else {
                    duplicateSnapshots += 1;
                    console.log(`Duplicate skipped ${tokenToSymbol[tokenText] || tokenText} ${expiryText}: strikes ${filteredCount}/${originalCount}`);
                }
            } else {
                filteredOutSnapshots += 1;
                console.log(`Skipped ${tokenToSymbol[tokenText] || tokenText} ${expiryText}: no strikes in range`);
            }

            receivedPairs.add(keyPair);

            if (targetPairs.size > 0 && remainingPairs() <= 0) {
                finish(true, 'All targets collected.');
            }
        });

        ws.on('close', (code, reason) => {
            ws = null;
            const closeReason = reason ? reason.toString() : '';

            if (finished) {
                console.log(`Connection closed (${code}) ${closeReason}`);
                console.log(`Summary: received ${receivedPairs.size}/${targetPairs.size} option-chain snapshots`);
                return;
            }

            scheduleReconnect(code, closeReason);
        });

        ws.on('error', (error) => {
            console.error(`WebSocket error: ${error.message}`);
        });
    }

    timeoutHandle = setTimeout(() => {
        finish(false, `Timeout (${config.timeoutMs}ms) reached before all targets were collected.`);
    }, config.timeoutMs);

    connect();
}

main().catch((error) => {
    console.error(`Fatal error: ${error.message}`);
    process.exit(1);
});
