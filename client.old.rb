# encoding: utf-8
require "socket"
require "multi_json"
require "digest/sha1"
require "hashie/mash"
require "xz"
require "zlib"

$path = 'archive--Sunnysoft--20091031.tar'
io = TCPSocket.new 'vps.martinkozak.net', 7835

# Creates initial message
io.puts MultiJson::dump({
    :type => :file,
    :path => $path + ".new",
    :size => File.size($path)
})

# Starts hash creation
file = File.open($path, 'r')
while data = file.read(1024 * 1024)
    io.puts MultiJson::dump({
        :type => :hash,
        :hash => Digest::SHA1.hexdigest(data)
    })
end

io.puts MultiJson::dump({
    :type => :hash,
    :end => true
})

# Process orders
file_bytes = 0

while message = io.gets
    message = Hashie::Mash::new(MultiJson::load(message))
    
    # Terminates if everything relevant received
    if message.end
        break
    end
    
    # Loads and sends requried block
    file.seek(message.sequence * 1024 * 1024)
    data = file.read(1024 * 1024)
    
    if not data.nil?
        #compressed = Zlib::Deflate::deflate(data, Zlib::BEST_COMPRESSION)
        compressed = XZ::compress(data)
        io.write "block" + [message.sequence, compressed.length].pack("QQ")
        io.write compressed
        
        file_bytes += data.length
        puts "#{file_bytes / (1024 * 1024)}M"
    end
end

# Terminates
io.puts MultiJson::dump({
    :type => :end
})

io.close()
file.close()
