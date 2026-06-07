//! Single-threaded epoll reactor for the 500MB Club telemetry API.
//!
//! Design: one non-blocking listener, a pool of client connections, and a
//! single pipelined Redis connection that multiplexes every client request.
//! Persistence is acknowledged before we answer (202 after XADD ack), so the
//! data is visible to the other replicas the instant the client is told "ok" —
//! correctness under round-robin without giving up the contract's async option.
//!
//! No libc, no threads, no per-request allocation. RSS tracks live concurrency.

const std = @import("std");
const linux = std.os.linux;

const http = @import("http.zig");
const json = @import("json.zig");
const redis = @import("redis.zig");
const sys = @import("sys.zig");
const Buf = @import("util.zig").Buf;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Per-replica connection slots. Concurrency here is throughput × latency
// (Little's law): with sub-millisecond-to-few-ms latencies, even thousands of
// RPS per replica imply only tens of concurrent upstream connections, so 384
// is generous headroom while capping worst-case retained buffer memory. Each
// slot keeps its buffers for reuse (no per-request allocation).
const MAX_CONN = 384;
const EVENTS = 256;
const READ_CHUNK = 16 * 1024;

const TAG_LISTEN: u64 = std.math.maxInt(u64);
const TAG_REDIS: u64 = std.math.maxInt(u64) - 1;

const EV_IN = linux.EPOLL.IN;
const EV_OUT = linux.EPOLL.OUT;
const EV_ERR = linux.EPOLL.ERR;
const EV_HUP = linux.EPOLL.HUP;
const EV_RDHUP = linux.EPOLL.RDHUP;

var instance_id: []const u8 = "api";
var stream_maxlen: u32 = 4096;

const alloc = std.heap.page_allocator;

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

const Metrics = struct {
    c2xx: u64 = 0,
    c4xx: u64 = 0,
    c5xx: u64 = 0,
    posts: u64 = 0,
    batches: u64 = 0,
    ranges: u64 = 0,
    anomalies: u64 = 0,
};
var metrics: Metrics = .{};

fn countStatus(code: u16) void {
    if (code < 400) metrics.c2xx += 1 else if (code < 500) metrics.c4xx += 1 else metrics.c5xx += 1;
}

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------

const Conn = struct {
    fd: i32 = -1,
    gen: u32 = 0,
    in_use: bool = false,
    inflight: bool = false, // an async redis request is pending for this conn
    want_close: bool = false,
    cur_events: u32 = 0,
    read: Buf,
    write: Buf,
};

const Kind = enum { post, batch, range, anomaly };

const Pending = struct {
    conn: u32 = 0,
    gen: u32 = 0,
    kind: Kind = .post,
    expected: u32 = 1,
    seen: u32 = 0,
    accepted: u32 = 0,
    limit: u32 = 100,
    offset: u32 = 0,
    keepalive: bool = true,
    failed: bool = false, // storage returned an error for this request
};

// Idle connections shrink read/write buffers above this back to the OS so a
// rare large range page can't permanently inflate pooled RSS. Set well above
// normal request/response sizes so steady traffic never reallocates.
const BUF_KEEP = 32 * 1024;

// Monotonic per-instance sequence appended to every stored member so that two
// identical samples never collapse into one ZSET entry. Seeded from the clock
// so distinct instances/restarts don't share a sequence space.
var write_seq: u64 = 0;

// Trim sampling: single POSTs trim only every TRIM_EVERY writes (see
// ingestSingle). Must be a power of two so the modulo folds to a mask.
const TRIM_EVERY: u32 = 16;
var trim_tick: u32 = 0;

// ---------------------------------------------------------------------------
// Global reactor state
// ---------------------------------------------------------------------------

var epfd: i32 = -1;
var listen_fd: i32 = -1;

var pool: [MAX_CONN]Conn = undefined;
var free_list: [MAX_CONN]u32 = undefined;
var free_top: usize = 0;
var active_conns: usize = 0;

// Pending FIFO ring (<= one entry per connection in flight).
var ring: [MAX_CONN + 1]Pending = undefined;
var ring_head: usize = 0;
var ring_tail: usize = 0;

fn ringCount() usize {
    return (ring_tail + ring.len - ring_head) % ring.len;
}
fn ringPush(p: Pending) void {
    ring[ring_tail] = p;
    ring_tail = (ring_tail + 1) % ring.len;
}
fn ringFront() ?*Pending {
    if (ring_head == ring_tail) return null;
    return &ring[ring_head];
}
fn ringPop() void {
    ring_head = (ring_head + 1) % ring.len;
}

// Redis connection.
const RedisState = enum { disconnected, connecting, handshaking, ready };
var redis_fd: i32 = -1;
var redis_state: RedisState = .disconnected;
var redis_cur_events: u32 = 0;
var rin: Buf = undefined; // inbound replies
var rout: Buf = undefined; // outbound pipeline
var redis_addr: sys.Addr = undefined;

// Scratch buffer for building JSON response bodies.
var scratch: Buf = undefined;

var g_shutdown: bool = false;
var shutdown_deadline: i64 = 0;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn main() !void {
    sys.loadEnv();
    loadConfig();
    installSignals();

    rin = Buf.init(alloc);
    rout = Buf.init(alloc);
    scratch = Buf.init(alloc);
    write_seq = @bitCast(sys.nowMs());

    for (&pool, 0..) |*c, i| {
        c.* = .{ .read = Buf.init(alloc), .write = Buf.init(alloc) };
        free_list[i] = @intCast(MAX_CONN - 1 - i);
    }
    free_top = MAX_CONN;

    epfd = try sys.epollCreate();
    try setupListener();
    redisConnect();

    var events: [EVENTS]sys.epoll_event = undefined;

    while (true) {
        const n = sys.epollWait(epfd, &events, 1000);
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const ev = events[i];
            const tag = ev.data.u64;
            if (tag == TAG_LISTEN) {
                doAccept();
            } else if (tag == TAG_REDIS) {
                handleRedisEvent(ev.events);
            } else {
                handleConnEvent(@intCast(tag), ev.events);
            }
        }

        flushRedis();

        // Reconnect on the timer tick (rate-limited to ~1/s by epoll timeout).
        // Handles the startup race where redis is not up yet, and recovery.
        if (redis_state == .disconnected) redisConnect();

        if (g_shutdown and shutdownReady()) break;
    }
}

fn loadConfig() void {
    if (sys.getenv("INSTANCE_ID")) |v| {
        instance_id = v;
    } else if (sys.getenv("HOSTNAME")) |v| {
        instance_id = v;
    }
    if (sys.getenv("STREAM_MAXLEN")) |v| {
        stream_maxlen = std.fmt.parseInt(u32, v, 10) catch stream_maxlen;
    }

    const host = sys.getenv("REDIS_HOST") orelse "127.0.0.1";
    const port_s = sys.getenv("REDIS_PORT") orelse "6379";
    const port = std.fmt.parseInt(u16, port_s, 10) catch 6379;
    redis_addr = resolve(host, port);
}

fn resolve(host: []const u8, port: u16) sys.Addr {
    // Hostnames require DNS, which we deliberately avoid (no libc): configure
    // REDIS_HOST as a literal IP (compose assigns redis a static address).
    return sys.parseIp4(host, port) orelse sys.ipv4(.{ 127, 0, 0, 1 }, port);
}

// ---------------------------------------------------------------------------
// Signals / shutdown
// ---------------------------------------------------------------------------

fn onTerm(_: sys.SIG) callconv(.c) void {
    g_shutdown = true;
}

fn installSignals() void {
    var sa = sys.Sigaction{
        .handler = .{ .handler = onTerm },
        .mask = sys.emptySigset(),
        .flags = 0,
    };
    sys.sigaction(sys.SIG.TERM, &sa);
    sys.sigaction(sys.SIG.INT, &sa);

    var ign = sys.Sigaction{
        .handler = .{ .handler = sys.SIG.IGN },
        .mask = sys.emptySigset(),
        .flags = 0,
    };
    sys.sigaction(sys.SIG.PIPE, &ign);
}

fn shutdownReady() bool {
    // Stop accepting; drain in-flight work (contract: within 10s).
    if (listen_fd >= 0) {
        epollDel(listen_fd);
        sys.close(listen_fd);
        listen_fd = -1;
        shutdown_deadline = sys.nowMs() + 9000;
    }
    // Proactively close idle keep-alive connections (nginx holds these open);
    // only connections with a request still in flight need to linger.
    var i: usize = 0;
    while (i < MAX_CONN) : (i += 1) {
        const c = &pool[i];
        if (c.in_use and !c.inflight and c.write.pendingLen() == 0) closeConn(c);
    }
    if (active_conns == 0 and ringCount() == 0) return true;
    return sys.nowMs() >= shutdown_deadline;
}

// ---------------------------------------------------------------------------
// Listener
// ---------------------------------------------------------------------------

fn setupListener() !void {
    const port_s = sys.getenv("PORT") orelse "8000";
    const port = std.fmt.parseInt(u16, port_s, 10) catch 8000;

    const fd = try sys.tcpSocket();
    sys.setReuseAddr(fd);

    const addr = sys.ipv4(.{ 0, 0, 0, 0 }, port);
    try sys.bind(fd, &addr);
    try sys.listen(fd, 1024);

    listen_fd = fd;
    epollAdd(fd, EV_IN, TAG_LISTEN);
}

fn doAccept() void {
    while (true) {
        switch (sys.accept(listen_fd)) {
            .again => return,
            .err => return,
            .fd => |fd| {
                sys.setNoDelay(fd);
                if (free_top == 0) {
                    sys.close(fd); // pool exhausted; shed load
                    continue;
                }
                free_top -= 1;
                const idx = free_list[free_top];
                const c = &pool[idx];
                c.fd = fd;
                c.in_use = true;
                c.inflight = false;
                c.want_close = false;
                c.read.reset();
                c.write.reset();
                c.cur_events = EV_IN | EV_RDHUP;
                active_conns += 1;
                epollAdd(fd, c.cur_events, idx);
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Connection I/O
// ---------------------------------------------------------------------------

fn handleConnEvent(idx: u32, events: u32) void {
    const c = &pool[idx];
    if (!c.in_use) return;

    if (events & (EV_ERR | EV_HUP) != 0) {
        closeConn(c);
        return;
    }
    if (events & EV_IN != 0) {
        if (!onReadable(c)) return;
    }
    if (events & EV_OUT != 0) {
        if (!flushWrite(c)) return;
    }
    if (events & EV_RDHUP != 0) {
        if (c.write.pendingLen() == 0 and !c.inflight) {
            closeConn(c);
            return;
        }
    }
}

/// Returns false if the connection was closed.
fn onReadable(c: *Conn) bool {
    while (true) {
        c.read.ensureUnused(READ_CHUNK) catch {
            closeConn(c);
            return false;
        };
        const dst = c.read.writable();
        switch (sys.recv(c.fd, dst)) {
            .again => break,
            .closed => {
                closeConn(c);
                return false;
            },
            .n => |r| {
                c.read.advance(r);
                if (r < dst.len) break; // socket drained
            },
        }
    }

    processConn(c);
    if (!c.in_use) return false;
    return flushWrite(c);
}

/// Parse and dispatch buffered requests (one in flight per conn keeps order).
fn processConn(c: *Conn) void {
    while (!c.inflight and c.in_use) {
        const res = http.parse(c.read.pending());
        switch (res) {
            .incomplete => break,
            .bad => |code| {
                writeResp(c, code, null, "", false);
                c.want_close = true;
                c.read.reset();
                break;
            },
            .ok => |req| {
                const ka = req.keepalive;
                dispatch(c, req);
                c.read.consume(req.total_len);
                if (!ka) {
                    c.want_close = true;
                    if (!c.inflight) break;
                }
            },
        }
    }
}

/// Returns false if the connection was closed.
fn flushWrite(c: *Conn) bool {
    while (c.write.pendingLen() > 0) {
        const data = c.write.pending();
        switch (sys.send(c.fd, data)) {
            .again => {
                updateConnEvents(c, true);
                return true;
            },
            .closed => {
                closeConn(c);
                return false;
            },
            .n => |n| c.write.consume(n),
        }
    }
    updateConnEvents(c, false);
    if (c.want_close and !c.inflight) {
        closeConn(c);
        return false;
    }
    // Connection is idle and fully drained: return any oversized buffer space.
    if (!c.inflight and c.read.pendingLen() == 0) {
        c.write.shrinkIfEmpty(BUF_KEEP);
        c.read.shrinkIfEmpty(BUF_KEEP);
    }
    return true;
}

fn updateConnEvents(c: *Conn, want_out: bool) void {
    var ev: u32 = EV_IN | EV_RDHUP;
    if (want_out) ev |= EV_OUT;
    if (ev != c.cur_events) {
        c.cur_events = ev;
        epollMod(c.fd, ev, indexOf(c));
    }
}

fn closeConn(c: *Conn) void {
    if (!c.in_use) return;
    epollDel(c.fd);
    sys.close(c.fd);
    c.in_use = false;
    c.fd = -1;
    c.gen +%= 1; // invalidate any in-flight pending entry
    c.read.reset();
    c.write.reset();
    active_conns -= 1;
    free_list[free_top] = indexOf(c);
    free_top += 1;
}

inline fn indexOf(c: *Conn) u32 {
    const base = @intFromPtr(&pool[0]);
    return @intCast((@intFromPtr(c) - base) / @sizeOf(Conn));
}

// ---------------------------------------------------------------------------
// Request dispatch / routing
// ---------------------------------------------------------------------------

fn dispatch(c: *Conn, req: http.Request) void {
    var path = req.target;
    var query: []const u8 = "";
    if (std.mem.indexOfScalar(u8, req.target, '?')) |q| {
        path = req.target[0..q];
        query = req.target[q + 1 ..];
    }

    if (req.method == .get) {
        if (std.mem.eql(u8, path, "/healthz")) return writeResp(c, 200, "text/plain", "ok", req.keepalive);
        if (std.mem.eql(u8, path, "/readyz")) {
            if (redis_state == .ready)
                return writeResp(c, 200, "text/plain", "ok", req.keepalive);
            return writeResp(c, 503, "text/plain", "not ready", req.keepalive);
        }
        if (std.mem.eql(u8, path, "/metrics")) return writeMetrics(c, req.keepalive);
    }

    const prefix = "/devices/";
    if (!std.mem.startsWith(u8, path, prefix)) return writeResp(c, 404, null, "", req.keepalive);
    const rest = path[prefix.len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return writeResp(c, 404, null, "", req.keepalive);
    const id = rest[0..slash];
    const tail = rest[slash..];

    if (!http.validDeviceId(id)) return writeResp(c, 400, null, "", req.keepalive);

    if (std.mem.eql(u8, tail, "/telemetry")) {
        if (req.method == .post) return ingestSingle(c, id, req);
        if (req.method == .get) return queryRange(c, id, query, req.keepalive);
        return writeResp(c, 404, null, "", req.keepalive);
    }
    if (std.mem.eql(u8, tail, "/telemetry/batch")) {
        if (req.method == .post) return ingestBatch(c, id, req);
        return writeResp(c, 404, null, "", req.keepalive);
    }
    if (std.mem.eql(u8, tail, "/anomaly")) {
        if (req.method == .get) return queryAnomaly(c, id, req.keepalive);
        return writeResp(c, 404, null, "", req.keepalive);
    }
    return writeResp(c, 404, null, "", req.keepalive);
}

fn deviceKey(id: []const u8, buf: *[66]u8) []const u8 {
    buf[0] = 'z';
    buf[1] = ':';
    @memcpy(buf[2 .. 2 + id.len], id);
    return buf[0 .. 2 + id.len];
}

const MEMBER_SIZE = json.RECORD_SIZE + 8;

/// Packed record + unique sequence -> ZSET member bytes.
fn buildMember(p: json.Point, buf: *[MEMBER_SIZE]u8) []const u8 {
    var rec: [json.RECORD_SIZE]u8 = undefined;
    json.pack(p, &rec);
    @memcpy(buf[0..json.RECORD_SIZE], &rec);
    write_seq +%= 1;
    std.mem.writeInt(u64, buf[json.RECORD_SIZE..][0..8], write_seq, .little);
    return buf[0..MEMBER_SIZE];
}

/// ZADD <key> <ts> <member>
fn appendZadd(key: []const u8, ts: i64, member: []const u8) !void {
    try redis.arrayHeader(&rout, 4);
    try redis.bulk(&rout, "ZADD");
    try redis.bulk(&rout, key);
    try redis.bulkInt(&rout, ts);
    try redis.bulk(&rout, member);
}

/// ZREMRANGEBYRANK <key> 0 -(cap+1)  — keep only the newest `cap` samples.
fn appendTrim(key: []const u8) !void {
    try redis.arrayHeader(&rout, 4);
    try redis.bulk(&rout, "ZREMRANGEBYRANK");
    try redis.bulk(&rout, key);
    try redis.bulkInt(&rout, 0);
    try redis.bulkInt(&rout, -@as(i64, stream_maxlen) - 1);
}

fn requireRedis(c: *Conn, keepalive: bool) bool {
    if (redis_state != .ready) {
        writeResp(c, 503, null, "", keepalive);
        return false;
    }
    return true;
}

fn ingestSingle(c: *Conn, id: []const u8, req: http.Request) void {
    const p = json.parseSingle(req.body) catch {
        return writeResp(c, 400, null, "", req.keepalive);
    };
    if (!requireRedis(c, req.keepalive)) return;

    var kb: [66]u8 = undefined;
    const key = deviceKey(id, &kb);
    var mb: [MEMBER_SIZE]u8 = undefined;
    const member = buildMember(p, &mb);

    // A single point adds one element, so trimming on every single POST doubles
    // Redis's command load for the most common operation. Trim only every
    // TRIM_EVERY-th single instead: a device's set overshoots its cap by at
    // most ~TRIM_EVERY before the next trim (negligible vs STREAM_MAXLEN, and
    // bounded hard by redis maxmemory). Batches still trim every time since one
    // request can add up to 100 points.
    trim_tick +%= 1;
    const do_trim = (trim_tick & (TRIM_EVERY - 1)) == 0;

    const ok = blk: {
        appendZadd(key, p.ts, member) catch break :blk false;
        if (do_trim) appendTrim(key) catch break :blk false;
        break :blk true;
    };
    if (!ok) return writeResp(c, 500, null, "", req.keepalive);

    // expected replies: ZADD ack, plus the trim ack when we trimmed.
    ringPush(.{ .conn = indexOf(c), .gen = c.gen, .kind = .post, .expected = if (do_trim) 2 else 1, .keepalive = req.keepalive });
    c.inflight = true;
    metrics.posts += 1;
}

fn ingestBatch(c: *Conn, id: []const u8, req: http.Request) void {
    var pts: [json.MAX_BATCH]json.Point = undefined;
    const n = json.parseBatch(req.body, &pts) catch |e| {
        const code: u16 = if (e == json.ParseError.TooManyPoints) 413 else 400;
        return writeResp(c, code, null, "", req.keepalive);
    };
    if (!requireRedis(c, req.keepalive)) return;

    var kb: [66]u8 = undefined;
    const key = deviceKey(id, &kb);

    // One ZADD with all score/member pairs, then a single trim.
    (blk: {
        redis.arrayHeader(&rout, 2 + 2 * n) catch break :blk error.Oom;
        redis.bulk(&rout, "ZADD") catch break :blk error.Oom;
        redis.bulk(&rout, key) catch break :blk error.Oom;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            var mb: [MEMBER_SIZE]u8 = undefined;
            const member = buildMember(pts[i], &mb);
            redis.bulkInt(&rout, pts[i].ts) catch break :blk error.Oom;
            redis.bulk(&rout, member) catch break :blk error.Oom;
        }
        appendTrim(key) catch break :blk error.Oom;
        break :blk {};
    }) catch return writeResp(c, 500, null, "", req.keepalive);

    // expected = 2 replies: ZADD count + trim ack. accepted comes from the ZADD.
    ringPush(.{ .conn = indexOf(c), .gen = c.gen, .kind = .batch, .expected = 2, .keepalive = req.keepalive });
    c.inflight = true;
    metrics.batches += 1;
}

fn queryRange(c: *Conn, id: []const u8, query: []const u8, keepalive: bool) void {
    const from = getParam(query, "from") orelse return writeResp(c, 400, null, "", keepalive);
    const to = getParam(query, "to") orelse return writeResp(c, 400, null, "", keepalive);

    const from_v = std.fmt.parseInt(i64, from, 10) catch return writeResp(c, 400, null, "", keepalive);
    const to_v = std.fmt.parseInt(i64, to, 10) catch return writeResp(c, 400, null, "", keepalive);
    if (from_v > to_v) return writeResp(c, 400, null, "", keepalive);

    var limit: u32 = 100;
    if (getParam(query, "limit")) |ls| {
        limit = std.fmt.parseInt(u32, ls, 10) catch return writeResp(c, 400, null, "", keepalive);
        if (limit < 1 or limit > 500) return writeResp(c, 400, null, "", keepalive);
    }

    // Cursor is an opaque numeric offset into the (stable, historical) window.
    var offset: u32 = 0;
    if (getParam(query, "cursor")) |cur| {
        offset = std.fmt.parseInt(u32, cur, 10) catch return writeResp(c, 400, null, "", keepalive);
    }

    if (!requireRedis(c, keepalive)) return;

    var kb: [66]u8 = undefined;
    const key = deviceKey(id, &kb);
    // ZRANGEBYSCORE key <from> <to> LIMIT <offset> <limit+1>  (one extra to detect more)
    (blk: {
        redis.arrayHeader(&rout, 7) catch break :blk error.Oom;
        redis.bulk(&rout, "ZRANGEBYSCORE") catch break :blk error.Oom;
        redis.bulk(&rout, key) catch break :blk error.Oom;
        redis.bulk(&rout, from) catch break :blk error.Oom;
        redis.bulk(&rout, to) catch break :blk error.Oom;
        redis.bulk(&rout, "LIMIT") catch break :blk error.Oom;
        redis.bulkInt(&rout, offset) catch break :blk error.Oom;
        redis.bulkInt(&rout, @as(i64, limit) + 1) catch break :blk error.Oom;
        break :blk {};
    }) catch return writeResp(c, 500, null, "", keepalive);

    ringPush(.{ .conn = indexOf(c), .gen = c.gen, .kind = .range, .expected = 1, .limit = limit, .offset = offset, .keepalive = keepalive });
    c.inflight = true;
    metrics.ranges += 1;
}

fn queryAnomaly(c: *Conn, id: []const u8, keepalive: bool) void {
    if (!requireRedis(c, keepalive)) return;
    var kb: [66]u8 = undefined;
    const key = deviceKey(id, &kb);
    // ZREVRANGE key 0 255 -> the 256 most-recent samples, newest first.
    (blk: {
        redis.arrayHeader(&rout, 4) catch break :blk error.Oom;
        redis.bulk(&rout, "ZREVRANGE") catch break :blk error.Oom;
        redis.bulk(&rout, key) catch break :blk error.Oom;
        redis.bulk(&rout, "0") catch break :blk error.Oom;
        redis.bulk(&rout, "255") catch break :blk error.Oom;
        break :blk {};
    }) catch return writeResp(c, 500, null, "", keepalive);
    ringPush(.{ .conn = indexOf(c), .gen = c.gen, .kind = .anomaly, .expected = 1, .keepalive = keepalive });
    c.inflight = true;
    metrics.anomalies += 1;
}

fn getParam(query: []const u8, name: []const u8) ?[]const u8 {
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |pair| {
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse continue;
        if (std.mem.eql(u8, pair[0..eq], name)) {
            const v = pair[eq + 1 ..];
            if (v.len == 0) return null;
            return v;
        }
    }
    return null;
}

// ---------------------------------------------------------------------------
// Redis connection handling
// ---------------------------------------------------------------------------

fn redisConnect() void {
    const fd = sys.tcpSocket() catch {
        redis_state = .disconnected;
        return;
    };
    sys.setNoDelay(fd);

    switch (sys.connect(fd, &redis_addr)) {
        .ok, .inprogress => {},
        .err => {
            sys.close(fd);
            redis_state = .disconnected;
            return;
        },
    }

    redis_fd = fd;
    redis_state = .connecting;
    redis_cur_events = EV_IN | EV_OUT | EV_RDHUP;
    epollAdd(fd, redis_cur_events, TAG_REDIS);
}

fn handleRedisEvent(events: u32) void {
    if (events & (EV_ERR | EV_HUP) != 0) {
        redisDown();
        return;
    }
    if (redis_state == .connecting and events & EV_OUT != 0) {
        redis_state = .handshaking;
        redis.ping(&rout) catch {};
    }
    if (events & EV_IN != 0) onRedisReadable();
    if (events & EV_OUT != 0) flushRedis();
}

fn onRedisReadable() void {
    while (true) {
        rin.ensureUnused(READ_CHUNK) catch {
            redisDown();
            return;
        };
        const dst = rin.writable();
        switch (sys.recv(redis_fd, dst)) {
            .again => break,
            .closed => {
                redisDown();
                return;
            },
            .n => |r| {
                rin.advance(r);
                if (r < dst.len) break;
            },
        }
    }

    if (redis_state == .handshaking) {
        const in = rin.pending();
        const rlen = redis.replyLen(in) orelse return;
        rin.consume(rlen);
        redis_state = .ready;
    }
    if (redis_state == .ready) processReplies();
}

fn redisDown() void {
    if (redis_fd >= 0) {
        epollDel(redis_fd);
        sys.close(redis_fd);
        redis_fd = -1;
    }
    redis_state = .disconnected;

    while (ringFront()) |pe| {
        if (connValid(pe)) {
            const c = &pool[pe.conn];
            writeResp(c, 503, null, "", pe.keepalive);
            c.inflight = false;
            _ = flushWrite(c);
        }
        ringPop();
    }
    rin.reset();
    rout.reset();
    // Reconnection happens on the next loop tick (state == .disconnected),
    // which rate-limits retries and avoids a busy spin when redis is down.
}

fn flushRedis() void {
    if (redis_fd < 0) return;
    while (rout.pendingLen() > 0) {
        const data = rout.pending();
        switch (sys.send(redis_fd, data)) {
            .again => break,
            .closed => {
                redisDown();
                return;
            },
            .n => |n| rout.consume(n),
        }
    }
    const want_out = rout.pendingLen() > 0 or redis_state == .connecting;
    var ev: u32 = EV_IN | EV_RDHUP;
    if (want_out) ev |= EV_OUT;
    if (ev != redis_cur_events) {
        redis_cur_events = ev;
        epollMod(redis_fd, ev, TAG_REDIS);
    }
}

inline fn connValid(pe: *const Pending) bool {
    const c = &pool[pe.conn];
    return c.in_use and c.gen == pe.gen;
}

/// Drain complete replies, matching them to pending entries in FIFO order.
fn processReplies() void {
    while (ringFront()) |pe| {
        const in = rin.pending();
        const rlen = redis.replyLen(in) orelse break;
        const reply = in[0..rlen];
        const valid = connValid(pe);

        switch (pe.kind) {
            .post => {
                // First reply is the ZADD ack; a storage error must not be
                // masked as a successful 202.
                if (pe.seen == 0 and redis.ackKind(reply) == .err) pe.failed = true;
                pe.seen += 1;
            },
            .batch => {
                if (pe.seen == 0) {
                    if (redis.ackKind(reply) == .err) pe.failed = true else pe.accepted = @intCast(@max(0, redis.parseInteger(reply)));
                }
                pe.seen += 1;
            },
            .range => {
                if (valid) buildRange(&pool[pe.conn], reply, pe.limit, pe.offset, pe.keepalive);
                pe.seen = pe.expected;
            },
            .anomaly => {
                if (valid) buildAnomaly(&pool[pe.conn], reply, pe.keepalive);
                pe.seen = pe.expected;
            },
        }
        rin.consume(rlen);

        if (pe.seen < pe.expected) continue;

        if (valid) {
            const c = &pool[pe.conn];
            switch (pe.kind) {
                .post => writeResp(c, if (pe.failed) 503 else 202, null, "", pe.keepalive),
                .batch => if (pe.failed) writeResp(c, 503, null, "", pe.keepalive) else writeBatchResp(c, pe.accepted, pe.keepalive),
                else => {},
            }
            c.inflight = false;
            processConn(c);
            if (c.in_use) _ = flushWrite(c);
        }
        ringPop();
    }
}

// ---------------------------------------------------------------------------
// Response building
// ---------------------------------------------------------------------------

fn statusLine(code: u16) []const u8 {
    return switch (code) {
        200 => "HTTP/1.1 200 OK\r\n",
        202 => "HTTP/1.1 202 Accepted\r\n",
        400 => "HTTP/1.1 400 Bad Request\r\n",
        404 => "HTTP/1.1 404 Not Found\r\n",
        413 => "HTTP/1.1 413 Payload Too Large\r\n",
        431 => "HTTP/1.1 431 Request Header Fields Too Large\r\n",
        500 => "HTTP/1.1 500 Internal Server Error\r\n",
        503 => "HTTP/1.1 503 Service Unavailable\r\n",
        else => "HTTP/1.1 500 Internal Server Error\r\n",
    };
}

fn writeResp(c: *Conn, code: u16, ctype: ?[]const u8, body: []const u8, keepalive: bool) void {
    const w = &c.write;
    w.appendSlice(statusLine(code)) catch {
        c.want_close = true;
        return;
    };
    w.appendSlice("X-Instance-Id: ") catch {};
    w.appendSlice(instance_id) catch {};
    w.appendSlice("\r\n") catch {};
    if (ctype) |ct| {
        w.appendSlice("Content-Type: ") catch {};
        w.appendSlice(ct) catch {};
        w.appendSlice("\r\n") catch {};
    }
    w.appendSlice("Content-Length: ") catch {};
    w.appendInt(@intCast(body.len)) catch {};
    w.appendSlice("\r\n") catch {};
    if (!keepalive) {
        w.appendSlice("Connection: close\r\n") catch {};
        c.want_close = true;
    }
    w.appendSlice("\r\n") catch {};
    w.appendSlice(body) catch {};
    countStatus(code);
}

fn writeBatchResp(c: *Conn, accepted: u32, keepalive: bool) void {
    var body: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&body, "{{\"accepted\":{d}}}", .{accepted}) catch "{\"accepted\":0}";
    writeResp(c, 202, "application/json", s, keepalive);
}

fn buildRange(c: *Conn, reply: []const u8, limit: u32, offset: u32, keepalive: bool) void {
    scratch.reset();
    var it = redis.Members.init(reply);
    const total = it.count();
    const has_more = total > limit;
    const out_n = if (has_more) limit else @as(u32, @intCast(total));

    scratch.appendSlice("{\"points\":[") catch return oom(c, keepalive);
    var emitted: u32 = 0;
    while (emitted < out_n) {
        const member = it.next() orelse break;
        const p = json.unpack(member[0..@min(member.len, json.RECORD_SIZE)]) orelse continue;
        if (emitted > 0) scratch.appendByte(',') catch return oom(c, keepalive);
        scratch.ensureUnused(json.POINT_JSON_MAX) catch return oom(c, keepalive);
        const wn = json.writePoint(scratch.writable(), p);
        scratch.advance(wn);
        emitted += 1;
    }
    scratch.appendSlice("],\"next_cursor\":") catch return oom(c, keepalive);
    if (has_more) {
        var tmp: [16]u8 = undefined;
        const s = std.fmt.bufPrint(&tmp, "\"{d}\"", .{offset + emitted}) catch return oom(c, keepalive);
        scratch.appendSlice(s) catch return oom(c, keepalive);
    } else {
        scratch.appendSlice("null") catch return oom(c, keepalive);
    }
    scratch.appendByte('}') catch return oom(c, keepalive);

    writeResp(c, 200, "application/json", scratch.pending(), keepalive);
}

fn buildAnomaly(c: *Conn, reply: []const u8, keepalive: bool) void {
    var it = redis.Members.init(reply);
    var mags: [256]f64 = undefined;
    var cnt: usize = 0;
    while (it.next()) |member| {
        const p = json.unpack(member[0..@min(member.len, json.RECORD_SIZE)]) orelse continue;
        const mag = @sqrt(p.ax * p.ax + p.ay * p.ay + p.az * p.az);
        if (cnt < mags.len) {
            mags[cnt] = mag;
            cnt += 1;
        }
    }

    if (cnt < 8) {
        writeResp(c, 404, null, "", keepalive);
        return;
    }

    var sum: f64 = 0;
    for (mags[0..cnt]) |m| sum += m;
    const mean = sum / @as(f64, @floatFromInt(cnt));
    var varsum: f64 = 0;
    for (mags[0..cnt]) |m| {
        const d = m - mean;
        varsum += d * d;
    }
    const stddev = @sqrt(varsum / @as(f64, @floatFromInt(cnt)));
    const recent = mags[0]; // XREVRANGE => most recent first
    const z = if (stddev > 0) (recent - mean) / stddev else 0;
    const anomalous = z > 3.0;

    var tmp: [320]u8 = undefined;
    const s = std.fmt.bufPrint(&tmp, "{{\"z_score\":{d},\"samples\":{d},\"anomalous\":{s},\"mean\":{d},\"stddev\":{d}}}", .{
        z, cnt, if (anomalous) "true" else "false", mean, stddev,
    }) catch return oom(c, keepalive);
    writeResp(c, 200, "application/json", s, keepalive);
}

fn oom(c: *Conn, keepalive: bool) void {
    writeResp(c, 500, null, "", keepalive);
}

fn writeMetrics(c: *Conn, keepalive: bool) void {
    scratch.reset();
    const w = &scratch;
    appendMetric(w, "# HELP http_requests_total Total HTTP responses by status class.\n");
    appendMetric(w, "# TYPE http_requests_total counter\n");
    appendCounter(w, "http_requests_total{class=\"2xx\"} ", metrics.c2xx);
    appendCounter(w, "http_requests_total{class=\"4xx\"} ", metrics.c4xx);
    appendCounter(w, "http_requests_total{class=\"5xx\"} ", metrics.c5xx);
    appendMetric(w, "# HELP telemetry_ops_total Telemetry operations by kind.\n");
    appendMetric(w, "# TYPE telemetry_ops_total counter\n");
    appendCounter(w, "telemetry_ops_total{op=\"post\"} ", metrics.posts);
    appendCounter(w, "telemetry_ops_total{op=\"batch\"} ", metrics.batches);
    appendCounter(w, "telemetry_ops_total{op=\"range\"} ", metrics.ranges);
    appendCounter(w, "telemetry_ops_total{op=\"anomaly\"} ", metrics.anomalies);
    appendMetric(w, "# HELP storage_up Whether storage is ready (1) or not (0).\n");
    appendMetric(w, "# TYPE storage_up gauge\n");
    appendCounter(w, "storage_up ", if (redis_state == .ready) @as(u64, 1) else 0);
    writeResp(c, 200, "text/plain; version=0.0.4", scratch.pending(), keepalive);
}

fn appendMetric(w: *Buf, s: []const u8) void {
    w.appendSlice(s) catch {};
}
fn appendCounter(w: *Buf, label: []const u8, v: u64) void {
    w.appendSlice(label) catch {};
    var tmp: [24]u8 = undefined;
    const s = std.fmt.bufPrint(&tmp, "{d}\n", .{v}) catch return;
    w.appendSlice(s) catch {};
}

// ---------------------------------------------------------------------------
// epoll helpers (bind the global epfd)
// ---------------------------------------------------------------------------

fn epollAdd(fd: i32, events: u32, tag: u64) void {
    sys.epollAdd(epfd, fd, events, tag);
}
fn epollMod(fd: i32, events: u32, tag: u64) void {
    sys.epollMod(epfd, fd, events, tag);
}
fn epollDel(fd: i32) void {
    sys.epollDel(epfd, fd);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test {
    std.testing.refAllDecls(@import("json.zig"));
    std.testing.refAllDecls(@import("util.zig"));
    std.testing.refAllDecls(@import("redis.zig"));
    std.testing.refAllDecls(@import("http.zig"));
}
