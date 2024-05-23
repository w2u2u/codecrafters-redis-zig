const std = @import("std");
const net = std.net;

const stdout = std.io.getStdOut().writer();

pub fn main() !void {
    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.print("accepted new connection\n", .{});

        const handler = try std.Thread.spawn(.{}, handleConnection, .{connection});
        _ = handler;
    }
}

pub fn handleConnection(connection: net.Server.Connection) !void {
    defer connection.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer: [1024]u8 = undefined;

    while (true) {
        const msg_size = try connection.stream.read(buffer[0..]);
        const msg = buffer[0..msg_size];

        try stdout.print("received message: {s}\n", .{msg});

        const command = try Command.parse(allocator, msg);

        switch (command.type) {
            CommandType.PING => try connection.stream.writeAll("+PONG\r\n"),
            else => try connection.stream.writeAll("-ERR unknown command\r\n"),
        }
    }
}

const CommandType = enum {
    PING,
    UNKNOWN,
};

const Command = struct {
    type: CommandType,
    args: [][]u8 = undefined,

    pub fn parse(allocator: std.mem.Allocator, msg: []u8) !Command {
        var split_msg = std.mem.split(u8, msg, "\r\n");

        // Skip array size
        _ = split_msg.next();

        // Skip string size
        _ = split_msg.next();

        const command_type = split_msg.next() orelse return Command{ .type = CommandType.UNKNOWN };
        var uppercase_command_type = try allocator.alloc(u8, command_type.len);
        defer allocator.free(uppercase_command_type);

        for (command_type, 0..) |c, i| {
            uppercase_command_type[i] = std.ascii.toUpper(c);
        }

        if (std.mem.eql(u8, uppercase_command_type, "PING")) {
            return Command{ .type = CommandType.PING };
        } else {
            return Command{ .type = CommandType.UNKNOWN };
        }
    }
};
