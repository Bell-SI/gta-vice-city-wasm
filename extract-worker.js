// Streaming extract worker (drop-in replacement for public/extract-worker.js).
// Same protocol as the original: receives { file }, posts { type: 'progress'|'done'|'error', ... },
// writes the "_game_ready" marker ("v4") at the OPFS root last.
// Memory use stays around one decompressed chunk (~64 KB-1 MB) no matter how big the archive is.

class Reader {
  constructor(r) { this.r = r; this.cur = new Uint8Array(0); this.pos = 0; this.eof = false; }
  async fill() {
    while (this.pos >= this.cur.length && !this.eof) {
      const { value, done } = await this.r.read();
      if (done) { this.eof = true; break; }
      this.cur = value; this.pos = 0;
    }
    return this.pos < this.cur.length;
  }
  async take(max) {
    if (!(await this.fill())) return null;
    const end = Math.min(this.cur.length, this.pos + max);
    const out = this.cur.subarray(this.pos, end);
    this.pos = end;
    return out;
  }
  async readExact(n) {
    const out = new Uint8Array(n);
    let got = 0;
    while (got < n) {
      const p = await this.take(n - got);
      if (!p) break;
      out.set(p, got);
      got += p.length;
    }
    return got < n ? out.subarray(0, got) : out;
  }
  async skip(n) {
    while (n > 0) {
      const p = await this.take(n);
      if (!p) break;
      n -= p.length;
    }
  }
}

const dec = new TextDecoder();

function cstr(bytes, start, len) {
  let end = start;
  const max = start + len;
  while (end < max && bytes[end] !== 0) end++;
  return dec.decode(bytes.subarray(start, end));
}

function parseSize(b, start, len) {
  if (b[start] & 0x80) { // base-256 (GNU) for very large files
    let v = 0;
    for (let i = start + 1; i < start + len; i++) v = v * 256 + b[i];
    return v;
  }
  const s = cstr(b, start, len).trim();
  return s ? parseInt(s, 8) || 0 : 0;
}

function shouldIgnore(name) {
  if (!name) return true;
  const base = name.split('/').pop();
  return base === '.DS_Store' || base.startsWith('._');
}

const dirCache = new Map();
async function dirFor(root, parts) {
  let key = '';
  let dir = root;
  for (const p of parts) {
    key += '/' + p;
    let d = dirCache.get(key);
    if (!d) { d = await dir.getDirectoryHandle(p, { create: true }); dirCache.set(key, d); }
    dir = d;
  }
  return dir;
}

async function openFile(root, path) {
  const parts = path.split('/').filter(Boolean);
  const fileName = parts.pop();
  const dir = await dirFor(root, parts);
  return dir.getFileHandle(fileName, { create: true });
}

// Returns { write(chunk), close() }
async function openSink(root, path) {
  const fh = await openFile(root, path);
  if (typeof fh.createSyncAccessHandle === 'function') {
    try {
      const h = await fh.createSyncAccessHandle();
      h.truncate(0);
      let off = 0;
      return {
        async write(c) { h.write(c, { at: off }); off += c.length; },
        async close() { try { h.flush(); } finally { h.close(); } }
      };
    } catch (_) { /* fall through to createWritable */ }
  }
  const w = await fh.createWritable();
  return { async write(c) { await w.write(c); }, async close() { await w.close(); } };
}

async function writeSmall(root, path, bytes) {
  const s = await openSink(root, path);
  await s.write(bytes);
  await s.close();
}

self.onmessage = async (e) => {
  const { file } = e.data;
  let sink = null;
  try {
    const root = await navigator.storage.getDirectory();
    const total = file.size || 1;
    let read = 0;
    let lastPct = -1;

    const counter = new TransformStream({
      transform(chunk, ctl) {
        read += chunk.byteLength;
        const pct = Math.min(99, Math.floor((read / total) * 100));
        if (pct !== lastPct) { lastPct = pct; postMessage({ type: 'progress', phase: 'reading', pct }); }
        ctl.enqueue(chunk);
      }
    });

    const rd = new Reader(file.stream().pipeThrough(counter).pipeThrough(new DecompressionStream('gzip')).getReader());
    let longName = null;

    while (true) {
      const h = await rd.readExact(512);
      if (h.length < 512) break;
      let zero = true;
      for (let i = 0; i < 512; i++) if (h[i] !== 0) { zero = false; break; }
      if (zero) break;

      const size = parseSize(h, 124, 12);
      const pad = (512 - (size % 512)) % 512;
      const type = h[156] === 0 ? '0' : String.fromCharCode(h[156]);

      if (type === 'L') { // GNU long name
        const nb = await rd.readExact(size);
        longName = dec.decode(nb).replace(/\0.*$/, '');
        await rd.skip(pad);
        continue;
      }

      let name = cstr(h, 0, 100);
      if (cstr(h, 257, 5) === 'ustar') {
        const prefix = cstr(h, 345, 155);
        if (prefix) name = prefix + '/' + name;
      }
      if (longName) { name = longName; longName = null; }

      if ((type !== '0' && type !== '7') || shouldIgnore(name)) {
        await rd.skip(size + pad);
        continue;
      }

      sink = await openSink(root, name);
      let left = size;
      while (left > 0) {
        const c = await rd.take(Math.min(left, 1 << 20));
        if (!c) throw new Error('Archive ended early (file is incomplete or corrupt)');
        await sink.write(c);
        left -= c.length;
      }
      await sink.close();
      sink = null;
      await rd.skip(pad);
    }

    await writeSmall(root, '_game_ready', new TextEncoder().encode('v4'));
    postMessage({ type: 'progress', phase: 'reading', pct: 100 });
    postMessage({ type: 'done' });
  } catch (err) {
    if (sink) { try { await sink.close(); } catch (_) {} }
    postMessage({ type: 'error', message: (err && err.message) || String(err) });
  }
};
