//! Tiny HTTP/1.1 request parser tuned for this API. No allocations: it returns
//! slices into the caller's read buffer. Handles keep-alive and Content-Length
//! framing, which is all nginx's upstream proxy needs.

const std = @import("std");

pub const MAX_HEADER = 8 * 1024;
pub const MAX_BODY = 64 * 1024; // bodies above this -> 413

pub const Method = enum { get, post, other };

pub const Request = struct {
    method: Method,
    target: []const u8, // path + optional query
    body: []const u8,
    total_len: usize, // bytes to consume from the read buffer
    keepalive: bool,
};

pub const Result = union(enum) {
    incomplete,
    /// A framing-level error; respond with this status then close.
    bad: u16,
    ok: Request,
};

pub fn parse(buf: []const u8) Result {
    const hdr_end = std.mem.indexOf(u8, buf, "\r\n\r\n") orelse {
        if (buf.len > MAX_HEADER) return .{ .bad = 431 };
        return .incomplete;
    };
    const head = buf[0..hdr_end];

    // Request line.
    const line_end = std.mem.indexOfScalar(u8, head, '\r') orelse return .{ .bad = 400 };
    const line = head[0..line_end];

    var sp1 = std.mem.indexOfScalar(u8, line, ' ') orelse return .{ .bad = 400 };
    const method_s = line[0..sp1];
    const rest = line[sp1 + 1 ..];
    const sp2 = std.mem.indexOfScalar(u8, rest, ' ') orelse return .{ .bad = 400 };
    const target = rest[0..sp2];
    const version = rest[sp2 + 1 ..];

    const method: Method = blk: {
        if (std.mem.eql(u8, method_s, "GET")) break :blk .get;
        if (std.mem.eql(u8, method_s, "POST")) break :blk .post;
        break :blk .other;
    };

    // HTTP/1.1 keeps alive by default; 1.0 closes unless told otherwise.
    var keepalive = std.mem.endsWith(u8, version, "1.1");

    // Scan headers we care about.
    var content_length: usize = 0;
    var have_cl = false;
    var hi: usize = line_end + 2;
    while (hi < head.len) {
        const nl = std.mem.indexOfScalarPos(u8, head, hi, '\r') orelse head.len;
        const hline = head[hi..nl];
        if (hline.len == 0) break;
        if (asciiHeaderIs(hline, "content-length")) {
            const v = headerValue(hline);
            content_length = std.fmt.parseInt(usize, v, 10) catch return .{ .bad = 400 };
            have_cl = true;
        } else if (asciiHeaderIs(hline, "connection")) {
            const v = headerValue(hline);
            if (asciiContainsCi(v, "close")) {
                keepalive = false;
            } else if (asciiContainsCi(v, "keep-alive")) {
                keepalive = true;
            }
        }
        hi = nl + 2;
    }
    _ = &sp1;

    if (have_cl and content_length > MAX_BODY) return .{ .bad = 413 };

    const body_start = hdr_end + 4;
    const total = body_start + content_length;
    if (buf.len < total) {
        if (total > MAX_HEADER + MAX_BODY) return .{ .bad = 413 };
        return .incomplete;
    }

    return .{ .ok = .{
        .method = method,
        .target = target,
        .body = buf[body_start..total],
        .total_len = total,
        .keepalive = keepalive,
    } };
}

fn asciiHeaderIs(hline: []const u8, name: []const u8) bool {
    const colon = std.mem.indexOfScalar(u8, hline, ':') orelse return false;
    const hn = hline[0..colon];
    if (hn.len != name.len) return false;
    for (hn, name) |a, b| {
        if (toLower(a) != b) return false;
    }
    return true;
}

fn headerValue(hline: []const u8) []const u8 {
    const colon = std.mem.indexOfScalar(u8, hline, ':') orelse return "";
    var v = hline[colon + 1 ..];
    while (v.len > 0 and (v[0] == ' ' or v[0] == '\t')) v = v[1..];
    while (v.len > 0 and (v[v.len - 1] == ' ' or v[v.len - 1] == '\t')) v = v[0 .. v.len - 1];
    return v;
}

fn asciiContainsCi(hay: []const u8, needle: []const u8) bool {
    if (needle.len > hay.len) return false;
    var i: usize = 0;
    while (i + needle.len <= hay.len) : (i += 1) {
        var ok = true;
        for (needle, 0..) |c, j| {
            if (toLower(hay[i + j]) != c) {
                ok = false;
                break;
            }
        }
        if (ok) return true;
    }
    return false;
}

inline fn toLower(c: u8) u8 {
    return if (c >= 'A' and c <= 'Z') c + 32 else c;
}

/// Validate a device id against ^[a-zA-Z0-9_-]{1,64}$.
pub fn validDeviceId(id: []const u8) bool {
    if (id.len < 1 or id.len > 64) return false;
    for (id) |c| {
        const ok = (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or
            (c >= '0' and c <= '9') or c == '_' or c == '-';
        if (!ok) return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "parse get" {
    const r = parse("GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n");
    try std.testing.expect(r == .ok);
    try std.testing.expectEqual(Method.get, r.ok.method);
    try std.testing.expectEqualStrings("/healthz", r.ok.target);
    try std.testing.expect(r.ok.keepalive);
}

test "parse post with body" {
    const raw = "POST /devices/d1/telemetry HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello";
    const r = parse(raw);
    try std.testing.expect(r == .ok);
    try std.testing.expectEqualStrings("hello", r.ok.body);
    try std.testing.expectEqual(raw.len, r.ok.total_len);
}

test "incomplete body" {
    const raw = "POST /x HTTP/1.1\r\nContent-Length: 10\r\n\r\nhi";
    try std.testing.expect(parse(raw) == .incomplete);
}

test "connection close" {
    const r = parse("GET /x HTTP/1.1\r\nConnection: close\r\n\r\n");
    try std.testing.expect(!r.ok.keepalive);
}

test "device id validation" {
    try std.testing.expect(validDeviceId("dev-1_A"));
    try std.testing.expect(!validDeviceId(""));
    try std.testing.expect(!validDeviceId("bad id"));
    try std.testing.expect(!validDeviceId("nó"));
}
