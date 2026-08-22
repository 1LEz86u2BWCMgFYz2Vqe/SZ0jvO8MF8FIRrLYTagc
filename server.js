require('dotenv').config();

const { WebSocketServer } = require('ws');
const http = require('http');

const server = http.createServer((req, res) => {
    if (req.url === '/' && req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('sniper is running');
    } else {
        res.writeHead(404);
        res.end();
    }
});

const broadcast = (payload) => {
    wss.clients.forEach(client => {
        if (client.readyState === 1 && client.isAuthenticated) {
            client.send(JSON.stringify(payload));
        }
    });
};

const WORKSHOP_APPIDS = [730];
const EXCLUDED_SECTIONS = [
    "PlayerLinkDetails",
    "workshop_browse",
];
const EXCLUDED_KEYS = [
    "views",
];
const currWorkshopStr = {};
const currWorkshopSections = {};

const splitJSONStream = (text) => {
    const results = [];
    let depth = 0, inStr = false, strChar = null, escape = false, start = null;
    for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        if (start === null) {
            if (ch === '{' || ch === '[') { start = i; depth = 1; inStr = false; escape = false; }
            continue;
        }
        if (inStr) {
            if (escape) escape = false;
            else if (ch === '\\') escape = true;
            else if (ch === strChar) inStr = false;
            continue;
        }
        if (ch === '"') { inStr = true; strChar = ch; continue; }
        if (ch === '{' || ch === '[') depth++;
        else if (ch === '}' || ch === ']') {
            depth--;
            if (depth === 0) { results.push(text.slice(start, i + 1)); start = null; }
        }
    }
    return results;
};

const isItemArray = (val) =>
    Array.isArray(val) && val.length > 0 && val.every((i) => i && typeof i === 'object' && 'publishedfileid' in i);

const stripKeys = (val) => {
    if (!EXCLUDED_KEYS.length) return val;
    if (Array.isArray(val)) return val.map(stripKeys);
    if (val && typeof val === 'object') {
        const copy = {};
        for (const [k, v] of Object.entries(val)) {
            if (EXCLUDED_KEYS.includes(k)) continue;
            copy[k] = v;
        }
        return copy;
    }
    return val;
};

const extractSections = (rawBody) => {
    const sections = {};
    for (const chunk of splitJSONStream(rawBody)) {
        let parsed;
        try {
            parsed = JSON.parse(chunk);
        } catch (e) {
            continue;
        }
        if (!parsed || typeof parsed !== 'object') continue;

        for (const [key, val] of Object.entries(parsed)) {
            if (EXCLUDED_SECTIONS.includes(key)) continue;
            if (isItemArray(val)) sections[key] = stripKeys(val);
        }

        if (typeof parsed.queryData === 'string') {
            let cache;
            try {
                cache = JSON.parse(parsed.queryData);
            } catch (e) {
                continue;
            }
            for (const q of cache.queries || []) {
                const data = q.state?.data;
                const sectionName = Array.isArray(q.queryKey) ? String(q.queryKey[0]) : q.queryHash;
                if (EXCLUDED_SECTIONS.includes(sectionName)) continue;
                if (data === undefined || data === null) continue;

                const label = Array.isArray(q.queryKey)
                    ? q.queryKey.map((k) => (typeof k === 'object' ? JSON.stringify(k) : String(k))).join('|')
                    : q.queryHash;

                if (isItemArray(data)) {
                    sections[label] = stripKeys(data);
                } else if (isItemArray(data.results)) {
                    sections[label] = stripKeys(data.results);
                } else {
                    sections[label] = stripKeys(data);
                }
            }
        }
    }
    return sections;
};

const checkWorkshop = async () => {
    for (const appid of WORKSHOP_APPIDS) {
        try {
            const res = await fetch(`https://steamcommunity.com/workshop/browse/?appid=${appid}&browsesort=accepted&section=mtxitems&special_filter=1&admin_view=1`, {
                headers: { 'x-valve-request-type': 'routeData' },
                method: 'GET',
            });
            if (!res.ok) continue;
            const rawBody = await res.text();
            const sections = extractSections(rawBody);
            const newStr = JSON.stringify(sections);
            if (newStr !== currWorkshopStr[appid]) {
                currWorkshopStr[appid] = newStr;
                currWorkshopSections[appid] = sections;
                broadcast({ type: 'workshop_update', appid, sections });
            }
        } catch (e) {
            console.log(e);
        }
    }
};
setInterval(checkWorkshop, 60 * 1e3);
checkWorkshop();

let currStoreAssets, currStoreStr;
const SAPI_KEY = process.env.SAPIKEY;
const checkStore = async () => {
    try {
        const res = await fetch(`https://api.steampowered.com/ISteamEconomy/GetAssetPrices/v1/?appid=730&key=${SAPI_KEY}`);

        if (!res.ok) return;
        const contentType = res.headers.get("content-type");
        if (!contentType || !contentType.includes("application/json")) return;

        const data = await res.json();
        if (data.result?.success) {
            const newAssetsStr = JSON.stringify(data.result.assets);
            if (newAssetsStr !== currStoreStr) {
                currStoreAssets = data.result.assets;
                currStoreStr = newAssetsStr;
                broadcast({ type: 'store_update', assets: currStoreAssets });
            }
        }
    } catch (e) {
        console.log(e);
    }
};
setInterval(checkStore, 1 * 1e3);
checkStore();

const wss = new WebSocketServer({ server });
const API_KEY = process.env.APIKEY;
const ENDPOINTS = [
    process.env.ENDPOINT2,
    process.env.ENDPOINT3,
    process.env.ENDPOINT4,
    process.env.ENDPOINT5,
];

let latestId, lastEvent, checkInt;

const startTracking = () => {
    if (checkInt) return;

    console.log(`Tracking started. Baseline: ${latestId}`);

    let currEndpointIndex = 0;
    const check = async () => {
        if (!latestId) return;

        const ENDPOINT = ENDPOINTS[currEndpointIndex];
        currEndpointIndex = (currEndpointIndex + 1) % ENDPOINTS.length;

        let activeId;
        for (let i = 1; i <= 1; i++) {
            const id = latestId + i;

            try {
                const res = await fetch(ENDPOINT + id);
                if (res.body) await res.body.cancel();

                if (res.status === 401) {
                    activeId = id;
                    if (activeId > latestId) {
                        console.log(`Found ID ${activeId} with ${ENDPOINT}`);
                    }
                } else if (res.status !== 400) {
                    console.log(`Something went wrong on ID ${id} (status: ${res.status})`);
                    return;
                }
            } catch (err) {
                console.log(`Fetch error on ID ${id} with ${ENDPOINT}:`, err);
                return;
            }
        }

        if (!activeId || activeId <= latestId) return;

        const nowSec = Math.floor(Date.now() / 1e3);

        const prevId = lastEvent ? lastEvent.id : latestId;
        const diffFromBase = activeId - latestId;
        const diffFromPrev = activeId - prevId;

        const deleted = diffFromBase < 0;
        const signBase = deleted ? '' : '+';
        const signPrev = diffFromPrev < 0 ? '' : '+';

        const batch = lastEvent &&
            (nowSec - lastEvent.timestamp < (60 * 10)) &&
            (lastEvent.deleted === deleted);

        if (batch) {
            console.log(`Current post ID moved to ${activeId} (${signBase}${diffFromBase} total, ${signPrev}${diffFromPrev} from prev)`);
            lastEvent.id = activeId;
            lastEvent.timestamp = nowSec;
            broadcast({ type: 'batch_update', activeId, deleted });
        } else {
            console.log(`Activity detected (Post ID: ${activeId}; ${signBase}${diffFromBase})`);
            lastEvent = {
                id: activeId,
                timestamp: nowSec,
                deleted
            };
            broadcast({ type: 'alert', activeId, deleted });
        }

        latestId = activeId;
    };

    const delay = 1;
    checkInt = setInterval(check, delay * 1e3);
};

const buildProtobufPayload = (opts = {}) => {
    const bytes = [];

    bytes.push(0x08, opts.include_hidden ? 1 : 0);
    bytes.push(0x10, opts.language || 0);

    if (opts.include_confirmation_count) bytes.push(0x18, 1);
    if (opts.include_pinned_counts) bytes.push(0x20, 1);

    bytes.push(0x28, opts.include_read ? 1 : 0);

    return Buffer.from(bytes).toString('base64');
}

let access_token;
const GetSteamNotifications = async () => {
    if (!access_token) return;

    const params = new URLSearchParams({
        access_token,
        input_protobuf_encoded: buildProtobufPayload(),
        // format: "json"
    });

    try {
        const res = await fetch(`https://api.steampowered.com/ISteamNotificationService/GetSteamNotifications/v1?${params.toString()}`);
        if (!res.ok) throw new Error(`HTTP Error: ${res.status}`);
        // const data = await res.json();
        // console.log("Steam Notifs API returned:", JSON.stringify(data, null, 2));
        // return data;
    } catch (err) {
        // console.log("Fetch for Steam Notifs failed:", err.message);
    }
}
setInterval(GetSteamNotifications, 30 * 1e3);

wss.on('connection', (ws) => {
    ws.isAuthenticated = false;

    ws.on('message', (data) => {
        try {
            const payload = JSON.parse(data);
            if (payload.type === 'ping') return;

            if (payload.type === 'auth_sync' && payload.key === API_KEY) {
                ws.isAuthenticated = true;
                latestId = payload.latestId;
                console.log(`Baseline synced to ${latestId}`);
                ws.send(JSON.stringify({ type: 'status', msg: `Tracking from ID: ${latestId}` }));
                startTracking();

            }

            if (payload.token) access_token = payload.token;

            if (payload.type === 'request_store_assets' && currStoreAssets) {
                ws.send(JSON.stringify({
                    type: 'store_update',
                    assets: currStoreAssets
                }));
            }

            if (payload.type === 'request_workshop_sections') {
                for (const appid of WORKSHOP_APPIDS) {
                    if (currWorkshopSections[appid]) {
                        ws.send(JSON.stringify({ type: 'workshop_update', appid, sections: currWorkshopSections[appid] }));
                    }
                }
            }
        } catch (e) { }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});