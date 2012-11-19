# encoqding: utf-8
$:.push(".")

require "socket"
require "thread"
require "logger"

require "./server/worker"

# Main server loop
server = TCPServer.open(7835)
loop do
    Thread.start(server.accept) do |client|
        begin
            handler = RbSync::Server::Worker::new(client)
            handler.handle!
        rescue Exception => e
            Logger::new(STDOUT).fatal { e.message }
            handler.close!
        end
    end
end
