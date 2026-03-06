const fs = require('fs');
const path = require('path');

function parseArgs(argv) {
    const args = {
        input: 'scrapped_data',
        output: 'csv_data/options_chain_all.csv'
    };

    for (let i = 2; i < argv.length; i += 1) {
        const key = argv[i];
        const value = argv[i + 1];

        if (key === '--input' && value) {
            args.input = value;
            i += 1;
            continue;
        }

        if (key === '--output' && value) {
            args.output = value;
            i += 1;
            continue;
        }
    }

    return args;
}

function toAbsPath(inputPath) {
    return path.isAbsolute(inputPath) ? inputPath : path.resolve(process.cwd(), inputPath);
}

function getInputFiles(inputArg) {
    const inputPath = toAbsPath(inputArg);

    if (!fs.existsSync(inputPath)) {
        throw new Error(`Input path does not exist: ${inputPath}`);
    }

    const stat = fs.statSync(inputPath);
    if (stat.isFile()) {
        return [inputPath];
    }

    return fs
        .readdirSync(inputPath)
        .map((name) => path.join(inputPath, name))
        .filter((filePath) => fs.statSync(filePath).isFile())
        .sort();
}

function serializeValue(value) {
    if (value === null || value === undefined) {
        return '';
    }

    if (Array.isArray(value)) {
        return value.join('|');
    }

    if (typeof value === 'object') {
        return JSON.stringify(value);
    }

    return value;
}

function parseMessagesFromFile(filePath) {
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);

    const parsed = [];
    let parseErrors = 0;

    lines.forEach((line, index) => {
        try {
            parsed.push({
                recordId: index + 1,
                message: JSON.parse(line)
            });
        } catch (error) {
            parseErrors += 1;
        }
    });

    if (parsed.length > 0) {
        return { parsed, parseErrors };
    }

    try {
        const json = JSON.parse(content);
        if (Array.isArray(json)) {
            return {
                parsed: json.map((message, index) => ({ recordId: index + 1, message })),
                parseErrors
            };
        }

        return {
            parsed: [{ recordId: 1, message: json }],
            parseErrors
        };
    } catch (error) {
        return {
            parsed: [],
            parseErrors: parseErrors + 1
        };
    }
}

function resolveRoot(message) {
    if (!message || typeof message !== 'object') {
        return null;
    }

    if (message.kind !== 3 && message.packetId !== 3) {
        return null;
    }

    const payload = message.payload || {};
    const token = String(message.token ?? payload.token ?? '');
    const expiry = String(message.expiry ?? payload.expiry ?? '');
    const data = payload.data || {};

    const tokenNode = data[token] || data[Number(token)] || Object.values(data)[0];
    if (!tokenNode || typeof tokenNode !== 'object') {
        return null;
    }

    const expiryNode = tokenNode[expiry] || tokenNode[String(expiry)] || Object.values(tokenNode)[0];
    if (!expiryNode || typeof expiryNode !== 'object') {
        return null;
    }

    if (!expiryNode.chain || typeof expiryNode.chain !== 'object') {
        return null;
    }

    return {
        token,
        expiry,
        root: expiryNode
    };
}

function flattenRecord(sourceFile, sourceRecord, message) {
    const resolved = resolveRoot(message);
    if (!resolved) {
        return [];
    }

    const { token, expiry, root } = resolved;
    const chain = root.chain || {};

    return Object.keys(chain).map((strikeKey) => {
        const strikeEntry = chain[strikeKey] || {};
        const ce = strikeEntry.CE || {};
        const pe = strikeEntry.PE || {};
        const greeks = strikeEntry.greeks || {};

        const row = {
            source_file: sourceFile,
            source_record: sourceRecord,
            token: token,
            expiry: expiry,
            strike: serializeValue(strikeEntry.strike ?? strikeKey),
            atm_strike: serializeValue(root.atm_strike),
            atm_iv: serializeValue(root.atm_iv),
            atm_iv_change: serializeValue(root.atm_iv_change),
            atm_iv_percentile: serializeValue(root.atm_iv_percentile),
            atm_ivp_type: serializeValue(root.atm_ivp_type),
            pcr: serializeValue(root.pcr),
            max_pain_strike: serializeValue(root.max_pain_strike),
            chain_pcr: serializeValue(strikeEntry.pcr),
            iv_change: serializeValue(strikeEntry.ivChange),
            callDelta: serializeValue(greeks.callDelta),
            gamma: serializeValue(greeks.gamma),
            impliedVolatility: serializeValue(greeks.impliedVolatility),
            putDelta: serializeValue(greeks.putDelta),
            theta: serializeValue(greeks.theta),
            vega: serializeValue(greeks.vega)
        };

        Object.keys(ce).forEach((key) => {
            row[`ce_${key}`] = serializeValue(ce[key]);
        });

        Object.keys(pe).forEach((key) => {
            row[`pe_${key}`] = serializeValue(pe[key]);
        });

        return row;
    });
}

function escapeCsv(value) {
    const stringValue = String(value ?? '');
    if (!/[",\n\r]/.test(stringValue)) {
        return stringValue;
    }

    return `"${stringValue.replace(/"/g, '""')}"`;
}

function buildHeaders(rows) {
    const preferred = [
        'source_file',
        'source_record',
        'token',
        'expiry',
        'strike',
        'atm_strike',
        'atm_iv',
        'atm_iv_change',
        'atm_iv_percentile',
        'atm_ivp_type',
        'pcr',
        'max_pain_strike',
        'chain_pcr',
        'iv_change',
        'callDelta',
        'gamma',
        'impliedVolatility',
        'putDelta',
        'theta',
        'vega',
        'ce_instrument_token',
        'ce_last_price',
        'ce_net_change',
        'ce_absolute_price_change',
        'ce_oi',
        'ce_oi_change',
        'ce_is_liquid',
        'ce_liquidity_warnings',
        'ce_volume',
        'ce_best_buy_price',
        'ce_best_sell_price',
        'ce_oi_change_quantity',
        'ce_intrinsic_value',
        'ce_intrinsic_value_from_spot',
        'ce_time_value',
        'ce_pop',
        'ce_breakeven',
        'ce_breakeven_percentage',
        'pe_instrument_token',
        'pe_last_price',
        'pe_net_change',
        'pe_absolute_price_change',
        'pe_oi',
        'pe_oi_change',
        'pe_is_liquid',
        'pe_liquidity_warnings',
        'pe_volume',
        'pe_best_buy_price',
        'pe_best_sell_price',
        'pe_oi_change_quantity',
        'pe_intrinsic_value',
        'pe_intrinsic_value_from_spot',
        'pe_time_value',
        'pe_pop',
        'pe_breakeven',
        'pe_breakeven_percentage'
    ];

    const all = new Set();
    rows.forEach((row) => {
        Object.keys(row).forEach((key) => all.add(key));
    });

    const tail = [...all].filter((key) => !preferred.includes(key)).sort();
    return [...preferred.filter((key) => all.has(key)), ...tail];
}

function writeCsv(outputPathArg, rows) {
    const outputPath = toAbsPath(outputPathArg);
    const outputDir = path.dirname(outputPath);
    fs.mkdirSync(outputDir, { recursive: true });

    if (!rows.length) {
        fs.writeFileSync(outputPath, '', 'utf8');
        return outputPath;
    }

    const headers = buildHeaders(rows);
    const csvLines = [headers.join(',')];

    rows.forEach((row) => {
        const line = headers.map((header) => escapeCsv(row[header] ?? '')).join(',');
        csvLines.push(line);
    });

    fs.writeFileSync(outputPath, csvLines.join('\n'), 'utf8');
    return outputPath;
}

function main() {
    const args = parseArgs(process.argv);
    const files = getInputFiles(args.input);

    let totalRows = 0;
    let totalMessages = 0;
    let totalParseErrors = 0;
    const rows = [];

    files.forEach((filePath) => {
        const sourceFile = path.basename(filePath);
        const { parsed, parseErrors } = parseMessagesFromFile(filePath);
        totalParseErrors += parseErrors;

        parsed.forEach((entry) => {
            totalMessages += 1;
            const rowItems = flattenRecord(sourceFile, entry.recordId, entry.message);
            totalRows += rowItems.length;
            rows.push(...rowItems);
        });
    });

    const outputPath = writeCsv(args.output, rows);

    console.log(`Input files: ${files.length}`);
    console.log(`Messages read: ${totalMessages}`);
    console.log(`Rows written: ${totalRows}`);
    console.log(`Parse errors skipped: ${totalParseErrors}`);
    console.log(`CSV output: ${outputPath}`);
}

try {
    main();
} catch (error) {
    console.error(`Conversion failed: ${error.message}`);
    process.exit(1);
}
