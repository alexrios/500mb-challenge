//! Small, dependency-free building blocks: a growable byte buffer with a
//! consume head (so it doubles as an outbound queue) and integer formatting.

const std = @import("std");

pub const Buf = struct {
    alloc: std.mem.Allocator,
    data: []u8 = &[_]u8{},
    head: usize = 0, // bytes already consumed from the front
    len: usize = 0, // bytes written

    pub fn init(alloc: std.mem.Allocator) Buf {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Buf) void {
        if (self.data.len > 0) self.alloc.free(self.data);
        self.* = .{ .alloc = self.alloc };
    }

    /// Bytes pending (written but not yet consumed).
    pub inline fn pending(self: *const Buf) []u8 {
        return self.data[self.head..self.len];
    }

    pub inline fn pendingLen(self: *const Buf) usize {
        return self.len - self.head;
    }

    /// Mark `n` pending bytes as consumed; compact to empty when fully drained.
    pub fn consume(self: *Buf, n: usize) void {
        self.head += n;
        if (self.head == self.len) {
            self.head = 0;
            self.len = 0;
        }
    }

    /// Reset to empty without freeing the backing storage.
    pub fn reset(self: *Buf) void {
        self.head = 0;
        self.len = 0;
    }

    /// When empty, release capacity above `keep` back to the OS. Used to stop a
    /// rare large response (e.g. a 500-point range page) from permanently
    /// inflating a pooled connection's RSS. No-op for buffers already ≤ keep,
    /// so normal traffic never churns.
    pub fn shrinkIfEmpty(self: *Buf, keep: usize) void {
        if (self.head != self.len) return; // not empty
        if (self.data.len <= keep) return;
        self.data = self.alloc.realloc(self.data, keep) catch self.data;
        self.head = 0;
        self.len = 0;
    }

    /// Guarantee at least `extra` writable bytes after `len`, compacting the
    /// consumed prefix first and growing (doubling) only if still needed.
    pub fn ensureUnused(self: *Buf, extra: usize) !void {
        if (self.len + extra <= self.data.len) return;

        // Reclaim the consumed prefix by sliding pending bytes to the front.
        if (self.head > 0) {
            const p = self.pendingLen();
            if (p > 0) std.mem.copyForwards(u8, self.data[0..p], self.data[self.head..self.len]);
            self.head = 0;
            self.len = p;
            if (self.len + extra <= self.data.len) return;
        }

        var new_cap: usize = if (self.data.len == 0) 4096 else self.data.len;
        while (new_cap < self.len + extra) new_cap *= 2;
        self.data = try self.alloc.realloc(self.data, new_cap);
    }

    /// Slice available for direct writes (e.g. recv target).
    pub inline fn writable(self: *Buf) []u8 {
        return self.data[self.len..];
    }

    pub inline fn advance(self: *Buf, n: usize) void {
        self.len += n;
    }

    pub fn appendSlice(self: *Buf, s: []const u8) !void {
        try self.ensureUnused(s.len);
        @memcpy(self.data[self.len .. self.len + s.len], s);
        self.len += s.len;
    }

    pub fn appendByte(self: *Buf, b: u8) !void {
        try self.ensureUnused(1);
        self.data[self.len] = b;
        self.len += 1;
    }

    pub fn appendInt(self: *Buf, v: i64) !void {
        var tmp: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&tmp, "{d}", .{v}) catch unreachable;
        try self.appendSlice(s);
    }
};

test "buf append and consume" {
    var b = Buf.init(std.testing.allocator);
    defer b.deinit();
    try b.appendSlice("hello ");
    try b.appendSlice("world");
    try std.testing.expectEqualStrings("hello world", b.pending());
    b.consume(6);
    try std.testing.expectEqualStrings("world", b.pending());
    try b.appendSlice("!");
    try std.testing.expectEqualStrings("world!", b.pending());
}

test "buf grows" {
    var b = Buf.init(std.testing.allocator);
    defer b.deinit();
    var i: usize = 0;
    while (i < 1000) : (i += 1) try b.appendSlice("0123456789");
    try std.testing.expectEqual(@as(usize, 10000), b.pendingLen());
}
