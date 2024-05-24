const std = @import("std");
const net = std.net;
const Allocator = std.mem.Allocator;

const stdout = std.io.getStdOut().writer();

const SimpleError = "-ERR unknown command\r\n";

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.print("accepted new connection\n", .{});

        const handler = try std.Thread.spawn(.{}, handleConnection, .{ allocator, connection });
        _ = handler;
    }
}

pub fn handleConnection(allocator: Allocator, connection: net.Server.Connection) !void {
    defer connection.stream.close();

    var buffer: [1024]u8 = undefined;

    while (true) {
        const msg_size = try connection.stream.read(buffer[0..]);
        const msg = buffer[0..msg_size];
        const command = try Command.parse(allocator, msg);

        switch (command.type) {
            CommandType.PING => try connection.stream.writeAll("+PONG\r\n"),
            CommandType.ECHO => {
                const firstArg = getFirstElement(command.args) orelse SimpleError;
                const response = if (std.mem.eql(u8, firstArg, SimpleError)) firstArg else try toBulkString(firstArg);

                try connection.stream.writeAll(response);
            },
            else => try connection.stream.writeAll("-ERR unknown command\r\n"),
        }
    }
}

const CommandType = enum {
    PING,
    ECHO,
    UNKNOWN,
};

const Command = struct {
    type: CommandType,
    args: std.ArrayList([]const u8),

    pub fn parse(allocator: Allocator, msg: []u8) !Command {
        var splitMsg = std.mem.split(u8, msg, "\r\n");
        var args = std.ArrayList([]const u8).init(allocator);

        const unknownCommand = Command{
            .type = CommandType.UNKNOWN,
            .args = args,
        };

        // Skip array size and command size
        _ = splitMsg.next();
        _ = splitMsg.next();

        const command_type = splitMsg.next() orelse return unknownCommand;
        var buffer: [1024]u8 = undefined;
        const uppercaseCommandType = std.ascii.upperString(&buffer, command_type);

        if (std.mem.eql(u8, uppercaseCommandType, "PING")) {
            return Command{
                .type = CommandType.PING,
                .args = args,
            };
        } else if (std.mem.eql(u8, uppercaseCommandType, "ECHO")) {
            while (splitMsg.next()) |arg| {
                if (std.mem.startsWith(u8, arg, "$")) continue;
                try args.append(arg);
            }

            return Command{
                .type = CommandType.ECHO,
                .args = args,
            };
        } else {
            return unknownCommand;
        }
    }
};

fn toBulkString(string: []const u8) ![]u8 {
    var buffer: [1024]u8 = undefined;
    return try std.fmt.bufPrint(&buffer, "${d}\r\n{s}\r\n", .{ string.len, string });
}

fn getFirstElement(arrayList: std.ArrayList([]const u8)) ?[]const u8 {
    if (arrayList.items.len == 0) return null;
    return arrayList.items[0];
}
