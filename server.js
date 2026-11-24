/* eslint-disable */
'use strict';

const http = require('node:http');
const { URL } = require('node:url');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 8000);
const HOST = '0.0.0.0';
const rooms = new Map();
const INSTANCE_ID = (process.env.HOSTNAME || '').trim()
  || `pid${process.pid}-${Math.random().toString(36).slice(2, 8)}`;

function readRoom(url) {
  const q = new URL(url, 'http://x');
  let r = (q.searchParams.get('room') || '').trim();
  if (!r) { const seg = q.pathname.split('/').filter(Boolean)[0] || ''; r = seg; }
  r = r.replace(/[^A-Za-z0-9_-]/g, '').slice(0, 32);
  return r || null;
}
function readRole(url) {
  const q = new URL(url, 'http://x');
  const role = (q.searchParams.get('role') || '').trim().toLowerCase();
  return role === 'guest' ? 'guest' : 'host';
}
function writeJSON(ws, obj) {
  if (!ws || ws.readyState !== 1) return;
  try { ws.send(JSON.stringify(obj)); } catch {}
}
function peerOf(side) { return side === 'host' ? 'guest' : 'host'; }

const server = http.createServer((req, res) => {
  const path = (req.url || '/').split('?')[0];
  if (path === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, iid: INSTANCE_ID }));
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('sbgf relay on instance ' + INSTANCE_ID);
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const url = req.url || '/';
  const room = readRoom(url);
  const role = readRole(url);
  if (!room) { writeJSON(ws, { t: 'err', msg: '缺少房间号' }); ws.close(4400, 'no room'); return; }

  let rec = rooms.get(room);
  if (!rec) { rec = { host: undefined, guest: undefined }; rooms.set(room, rec); }
  if (rec[role]) {
    writeJSON(ws, { t: 'err',
      msg: role === 'host' ? '这个房间号已被占用，请换一个' : '这个房间已经有人加入，请等对方重建或更换房间号',
      iid: INSTANCE_ID });
    ws.close(4409, 'room role taken'); return;
  }
  const existingPeer = rec[peerOf(role)];
  rec[role] = ws;

  ws.on('close', () => {
    if (rec[role] === ws) {
      rec[role] = undefined;
      writeJSON(rec[peerOf(role)], { t: 'peer-left', role });
      if (!rec.host && !rec.guest) rooms.delete(room);
    }
  });
  ws.on('error', () => { try { ws.close(); } catch {} });
  writeJSON(ws, { t: 'welcome', room, role, peer: !!existingPeer });
  if (existingPeer) { writeJSON(existingPeer, { t: 'peer-joined', role }); }

  ws.on('message', (raw) => {
    const peer = rec[peerOf(role)];
    if (!peer || peer.readyState !== 1) return;
    let obj; try { obj = JSON.parse(String(raw)); } catch { return; }
    if (!obj || typeof obj !== 'object' || typeof obj.t !== 'string') return;
    if (obj.t === 'hello') return;
    writeJSON(peer, obj);
  });
});

server.listen(PORT, HOST, () => {
  console.log(`sbgf relay listening on http://${HOST}:${PORT}  iid=${INSTANCE_ID}`);
});
