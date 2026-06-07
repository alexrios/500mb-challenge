//! Hand-written, allocation-free JSON handling for the fixed telemetry schema.
//! We never use a general reflective parser: the payload shape is known, so a
//! flat scanner over the bytes is both faster and lighter.

const std = @import("std");

pub const ParseError = error{
    Malformed,
    MissingField,
    OutOfRange,
    TooManyPoints,
    EmptyBatch,
};

/// A single telemetry sample. `battery` is optional in the contract.
pub const Point = struct {
    ts: i64,
    lat: f64,
    lon: f64,
    battery: f64,
    has_battery: bool,
    ax: f64,
    ay: f64,
    az: f64,
};

/// Packed on-the-wire record stored as a Redis stream field value.
/// Layout (little-endian): ts(i64) | flags(u8) | lat lon battery ax ay az (6×f64)
/// 8 + 1 + 48 = 57 bytes. Compact and trivially (de)serialized.
pub const RECORD_SIZE = 57;
const FLAG_BATTERY: u8 = 1;

pub fn pack(p: Point, out: *[RECORD_SIZE]u8) void {
    std.mem.writeInt(i64, out[0..8], p.ts, .little);
    out[8] = if (p.has_battery) FLAG_BATTERY else 0;
    writeF64(out[9..17], p.lat);
    writeF64(out[17..25], p.lon);
    writeF64(out[25..33], p.battery);
    writeF64(out[33..41], p.ax);
    writeF64(out[41..49], p.ay);
    writeF64(out[49..57], p.az);
}

pub fn unpack(buf: []const u8) ?Point {
    if (buf.len != RECORD_SIZE) return null;
    return .{
        .ts = std.mem.readInt(i64, buf[0..8], .little),
        .has_battery = (buf[8] & FLAG_BATTERY) != 0,
        .lat = readF64(buf[9..17]),
        .lon = readF64(buf[17..25]),
        .battery = readF64(buf[25..33]),
        .ax = readF64(buf[33..41]),
        .ay = readF64(buf[41..49]),
        .az = readF64(buf[49..57]),
    };
}

inline fn writeF64(dst: []u8, v: f64) void {
    std.mem.writeInt(u64, dst[0..8], @bitCast(v), .little);
}
inline fn readF64(src: []const u8) f64 {
    return @bitCast(std.mem.readInt(u64, src[0..8], .little));
}

// ---------------------------------------------------------------------------
// Scanner
// ---------------------------------------------------------------------------

const Scanner = struct {
    s: []const u8,
    i: usize = 0,

    fn skipWs(self: *Scanner) void {
        while (self.i < self.s.len) {
            switch (self.s[self.i]) {
                ' ', '\t', '\n', '\r' => self.i += 1,
                else => break,
            }
        }
    }

    fn peek(self: *Scanner) ?u8 {
        if (self.i >= self.s.len) return null;
        return self.s[self.i];
    }

    fn expect(self: *Scanner, c: u8) ParseError!void {
        self.skipWs();
        if (self.i >= self.s.len or self.s[self.i] != c) return ParseError.Malformed;
        self.i += 1;
    }

    /// Parse a JSON string assuming no escape sequences (true for this schema's
    /// keys and our numeric values). Returns the inner slice.
    fn parseKey(self: *Scanner) ParseError![]const u8 {
        self.skipWs();
        try self.expect('"');
        const start = self.i;
        while (self.i < self.s.len and self.s[self.i] != '"') : (self.i += 1) {
            if (self.s[self.i] == '\\') return ParseError.Malformed;
        }
        if (self.i >= self.s.len) return ParseError.Malformed;
        const key = self.s[start..self.i];
        self.i += 1; // closing quote
        return key;
    }

    /// Return the raw token slice of a number/literal value (stops at , } ] ws).
    fn rawValue(self: *Scanner) ParseError![]const u8 {
        self.skipWs();
        const start = self.i;
        while (self.i < self.s.len) {
            switch (self.s[self.i]) {
                ',', '}', ']', ' ', '\t', '\n', '\r' => break,
                else => self.i += 1,
            }
        }
        if (self.i == start) return ParseError.Malformed;
        return self.s[start..self.i];
    }
};

const KEY_BITS = struct {
    const ts: u8 = 1 << 0;
    const lat: u8 = 1 << 1;
    const lon: u8 = 1 << 2;
    const ax: u8 = 1 << 3;
    const ay: u8 = 1 << 4;
    const az: u8 = 1 << 5;
    const required: u8 = ts | lat | lon | ax | ay | az;
};

/// Parse a single telemetry object. `sc` must be positioned at the '{'.
fn parseObject(sc: *Scanner) ParseError!Point {
    try sc.expect('{');
    var p: Point = .{
        .ts = 0,
        .lat = 0,
        .lon = 0,
        .battery = 0,
        .has_battery = false,
        .ax = 0,
        .ay = 0,
        .az = 0,
    };
    var seen: u8 = 0;

    sc.skipWs();
    if (sc.peek() == '}') {
        sc.i += 1;
        return ParseError.MissingField;
    }

    while (true) {
        const key = try sc.parseKey();
        try sc.expect(':');
        const raw = try sc.rawValue();

        if (std.mem.eql(u8, key, "ts")) {
            p.ts = std.fmt.parseInt(i64, raw, 10) catch return ParseError.Malformed;
            if (p.ts <= 0) return ParseError.OutOfRange;
            seen |= KEY_BITS.ts;
        } else if (std.mem.eql(u8, key, "lat")) {
            p.lat = try parseFinite(raw);
            if (p.lat < -90 or p.lat > 90) return ParseError.OutOfRange;
            seen |= KEY_BITS.lat;
        } else if (std.mem.eql(u8, key, "lon")) {
            p.lon = try parseFinite(raw);
            if (p.lon < -180 or p.lon > 180) return ParseError.OutOfRange;
            seen |= KEY_BITS.lon;
        } else if (std.mem.eql(u8, key, "battery")) {
            // Optional. `null` is treated as absent.
            if (!std.mem.eql(u8, raw, "null")) {
                p.battery = try parseFinite(raw);
                if (p.battery < 0 or p.battery > 1) return ParseError.OutOfRange;
                p.has_battery = true;
            }
        } else if (std.mem.eql(u8, key, "ax")) {
            p.ax = try parseFinite(raw);
            seen |= KEY_BITS.ax;
        } else if (std.mem.eql(u8, key, "ay")) {
            p.ay = try parseFinite(raw);
            seen |= KEY_BITS.ay;
        } else if (std.mem.eql(u8, key, "az")) {
            p.az = try parseFinite(raw);
            seen |= KEY_BITS.az;
        }
        // Unknown keys are ignored (forward-compatible).

        sc.skipWs();
        const c = sc.peek() orelse return ParseError.Malformed;
        if (c == ',') {
            sc.i += 1;
            continue;
        }
        if (c == '}') {
            sc.i += 1;
            break;
        }
        return ParseError.Malformed;
    }

    if ((seen & KEY_BITS.required) != KEY_BITS.required) return ParseError.MissingField;
    return p;
}

fn parseFinite(raw: []const u8) ParseError!f64 {
    const v = std.fmt.parseFloat(f64, raw) catch return ParseError.Malformed;
    if (!std.math.isFinite(v)) return ParseError.OutOfRange;
    return v;
}

/// Parse a single-point telemetry body.
pub fn parseSingle(body: []const u8) ParseError!Point {
    var sc = Scanner{ .s = body };
    const p = try parseObject(&sc);
    sc.skipWs();
    // Trailing garbage after the object is malformed.
    if (sc.i != sc.s.len) return ParseError.Malformed;
    return p;
}

pub const MAX_BATCH = 100;

/// Parse a batch body `{"points":[ ... ]}` into `out`. Returns the count.
pub fn parseBatch(body: []const u8, out: *[MAX_BATCH]Point) ParseError!usize {
    var sc = Scanner{ .s = body };
    try sc.expect('{');
    const key = try sc.parseKey();
    if (!std.mem.eql(u8, key, "points")) return ParseError.Malformed;
    try sc.expect(':');
    try sc.expect('[');

    var n: usize = 0;
    sc.skipWs();
    if (sc.peek() == ']') return ParseError.EmptyBatch;

    while (true) {
        if (n >= MAX_BATCH) {
            // Need to know if it's >100 (413) vs exactly 100 then close.
            // Peek past whitespace: if more elements follow, it's too many.
            return ParseError.TooManyPoints;
        }
        out[n] = try parseObject(&sc);
        n += 1;

        sc.skipWs();
        const c = sc.peek() orelse return ParseError.Malformed;
        if (c == ',') {
            sc.i += 1;
            continue;
        }
        if (c == ']') {
            sc.i += 1;
            break;
        }
        return ParseError.Malformed;
    }

    if (n == 0) return ParseError.EmptyBatch;
    return n;
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

/// Append a point as JSON into a writer-like byte sink (manual, no std.io).
/// Returns the number of bytes written. `dst` must be large enough (~160B).
pub fn writePoint(dst: []u8, p: Point) usize {
    var w = FixedWriter{ .buf = dst };
    w.put("{\"ts\":");
    w.int(p.ts);
    w.put(",\"lat\":");
    w.float(p.lat);
    w.put(",\"lon\":");
    w.float(p.lon);
    if (p.has_battery) {
        w.put(",\"battery\":");
        w.float(p.battery);
    }
    w.put(",\"ax\":");
    w.float(p.ax);
    w.put(",\"ay\":");
    w.float(p.ay);
    w.put(",\"az\":");
    w.float(p.az);
    w.put("}");
    return w.n;
}

/// Maximum JSON size of one serialized point (generous upper bound).
pub const POINT_JSON_MAX = 200;

const FixedWriter = struct {
    buf: []u8,
    n: usize = 0,

    fn put(self: *FixedWriter, s: []const u8) void {
        @memcpy(self.buf[self.n .. self.n + s.len], s);
        self.n += s.len;
    }
    fn int(self: *FixedWriter, v: i64) void {
        const s = std.fmt.bufPrint(self.buf[self.n..], "{d}", .{v}) catch unreachable;
        self.n += s.len;
    }
    fn float(self: *FixedWriter, v: f64) void {
        const s = std.fmt.bufPrint(self.buf[self.n..], "{d}", .{v}) catch unreachable;
        self.n += s.len;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "parse single valid" {
    const body =
        \\{"ts":1715800000000,"lat":-23.5505,"lon":-46.6333,"battery":0.82,"ax":0.11,"ay":-0.04,"az":9.81}
    ;
    const p = try parseSingle(body);
    try std.testing.expectEqual(@as(i64, 1715800000000), p.ts);
    try std.testing.expect(p.has_battery);
    try std.testing.expectApproxEqAbs(@as(f64, 9.81), p.az, 1e-9);
}

test "parse single without battery" {
    const body =
        \\{"ts":1,"lat":0,"lon":0,"ax":0,"ay":0,"az":9.8}
    ;
    const p = try parseSingle(body);
    try std.testing.expect(!p.has_battery);
}

test "missing required field" {
    const body =
        \\{"lat":0,"lon":0,"ax":0,"ay":0,"az":9.8}
    ;
    try std.testing.expectError(ParseError.MissingField, parseSingle(body));
}

test "out of range lat" {
    const body =
        \\{"ts":1,"lat":200,"lon":0,"ax":0,"ay":0,"az":9.8}
    ;
    try std.testing.expectError(ParseError.OutOfRange, parseSingle(body));
}

test "non-positive ts" {
    const body =
        \\{"ts":0,"lat":0,"lon":0,"ax":0,"ay":0,"az":9.8}
    ;
    try std.testing.expectError(ParseError.OutOfRange, parseSingle(body));
}

test "pack roundtrip" {
    const p = Point{ .ts = 42, .lat = -23.5, .lon = -46.6, .battery = 0.5, .has_battery = true, .ax = 1, .ay = 2, .az = 9.8 };
    var rec: [RECORD_SIZE]u8 = undefined;
    pack(p, &rec);
    const q = unpack(&rec).?;
    try std.testing.expectEqual(p.ts, q.ts);
    try std.testing.expectEqual(p.has_battery, q.has_battery);
    try std.testing.expectEqual(p.lat, q.lat);
    try std.testing.expectEqual(p.az, q.az);
}

test "batch parse" {
    const body =
        \\{"points":[{"ts":1,"lat":0,"lon":0,"ax":0,"ay":0,"az":9.8},{"ts":2,"lat":0,"lon":0,"ax":0,"ay":0,"az":9.8}]}
    ;
    var pts: [MAX_BATCH]Point = undefined;
    const n = try parseBatch(body, &pts);
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expectEqual(@as(i64, 2), pts[1].ts);
}

test "empty batch" {
    const body =
        \\{"points":[]}
    ;
    var pts: [MAX_BATCH]Point = undefined;
    try std.testing.expectError(ParseError.EmptyBatch, parseBatch(body, &pts));
}

test "write point" {
    const p = Point{ .ts = 5, .lat = 1.5, .lon = 2.5, .battery = 0, .has_battery = false, .ax = 0, .ay = 0, .az = 9.8 };
    var buf: [POINT_JSON_MAX]u8 = undefined;
    const n = writePoint(&buf, p);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..n], "\"ts\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, buf[0..n], "battery") == null);
}
