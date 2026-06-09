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

let currStoreAssets, currStoreStr;
const SAPI_KEY = process.env.SAPIKEY;
const checkStore = async() => {
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
                wss.clients.forEach(client => {
                    if (client.readyState === 1 && client.isAuthenticated) {
                        client.send(JSON.stringify({
                            type: 'store_update',
                            assets: currStoreAssets
                        }));
                    }
                });
            }
        }
    } catch(e) { 
        console.log(e);
    }
};
setInterval(checkStore, 1*1e3);
checkStore();

const wss = new WebSocketServer({ server });
const API_KEY = process.env.APIKEY;
const ENDPOINTS = [
    process.env.ENDPOINT1,
    process.env.ENDPOINT2,
];

let latestId, lastEvent, checkInt;

const startTracking = () => {
    if (checkInt) return;

    console.log(`Tracking started. Baseline: ${latestId}`);

    let currEndpointIndex = 0;
    const check = async() => {
        if (!latestId) return;
        
        const ENDPOINT = ENDPOINTS[currEndpointIndex];
        currEndpointIndex = (currEndpointIndex + 1) % ENDPOINTS.length;

        let activeId;
        for (let i = -1; i <= 1; i++) {
            const id = latestId + i;
            
            try {
                const res = await fetch(ENDPOINT + id);
                if (res.body) await res.body.cancel();
                
                if (res.status === 401) {
                    activeId = id;
                } else if (res.status !== 400) {
                    console.log(`Something went wrong on ID ${id} (status: ${res.status})`);
                    return;
                }
            } catch (err) {
                console.log(`Fetch error on ID ${id}:`, err);
                return;
            }
        }

        if (!activeId || activeId === latestId) return;

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

            wss.clients.forEach(client => {
                if (client.readyState === 1 && client.isAuthenticated) {
                    client.send(JSON.stringify({
                        type: 'batch_update',
                        activeId,
                        deleted
                    }));
                }
            });
        } else {
            console.log(`Activity detected (Post ID: ${activeId}; ${signBase}${diffFromBase})`);
            lastEvent = {
                id: activeId,
                timestamp: nowSec,
                deleted
            };
            wss.clients.forEach(client => {
                if (client.readyState === 1 && client.isAuthenticated) {
                    client.send(JSON.stringify({
                        type: 'alert',
                        activeId,
                        deleted
                    }));
                }
            });
        }

        latestId = activeId;
    };

    const delay = 0.5;
    checkInt = setInterval(check, delay*1e3);
};

wss.on('connection', (ws) => {
    ws.isAuthenticated = false;

    ws.on('message', (data) => {
        try {
            const payload = JSON.parse(data);
            if (payload.type === 'ping') return;
            if (payload.type === 'auth_sync') {
                if (payload.key === API_KEY) {
                    ws.isAuthenticated = true;
                    latestId = payload.latestId;
                    console.log(`Baseline synced to ${latestId}`);
                    ws.send(JSON.stringify({ type: 'status', msg: `Tracking from ID: ${latestId}` }));
                    startTracking();
                } else {
                    ws.close();
                }
            }
            if (payload.type === 'request_store_assets' && currStoreAssets) {
                ws.send(JSON.stringify({
                    type: 'store_update',
                    assets: currStoreAssets
                }));
            }
        } catch (e) { }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});