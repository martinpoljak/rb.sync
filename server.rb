# encoqding: utf-8
require "socket"
require "thread"
require "multi_json"
require "hashie/mash"
require "fileutils"
require "digest/sha1"
require "hash-utils"
require "xz"

class Worker

    @io
    @file
    @file_lock
    @local_hashes
    @remote_hashes
    
    @file_bytes
    
    ##
    # Constructor.
    #
    
    def initialize(io)
        @io = io
        @file_bytes = 0
        @remote_hashes = Queue::new
        @local_hashes = Queue::new
    end
    
    ##
    # Reports current transaction state.
    #
    
    def report!(force = false)
        if force or @file_bytes % (1 * 1024 * 1024) == 0
            puts "#{@file_bytes / (1024 * 1024)}M"
        end
    end
    
    ##
    # Handles the connection.
    #
    
    def handle!
    
        # Starts dispatching orders
        self.dispatch_orders!
        
        # Handles input data
        self.handle_data!
        
    end
    
    ##
    # Handles data.
    #
    
    def handle_data!
        loop do
            prefix = @io.read(5)

            if prefix.nil?
                Kernel.sleep(0.01)
            elsif prefix == "block"
                self.handle_block(@io.read(16), @io)
            else
                result = self.handle_message(prefix + @io.gets)
                if result == :end
                    return
                end
            end
        end
    end
    
    ##
    # Handles received block.
    #
    
    def handle_block(meta, io)
        sequence, size = meta.unpack("QQ")
        
        self.file do |file|
            file.seek(sequence * 1024 * 1024)
            #data = Zlib::Inflate::inflate(@io.read(size))
            data = XZ::decompress(@io.read(size))
            file.write(data)
            
            @file_bytes += data.length
            self.report!
        end
    end
    
    ##
    # Handles message.
    #
    
    def handle_message(data)
        message = Hashie::Mash::new(MultiJson::load(data))

        # Calls processing method according to incoming data
        case message.type.to_sym
            when :file
                self.load_file(message)
            when :hash
                self.add_hash(message)
            when :end
                return self.terminate!
        end
    end
    
    ##
    # Terminates processing.
    #
    
    def terminate!
        @io.close()
        
        self.file do |file|
            file.close()
        end
        
        puts "#{@file_bytes / (1024 * 1024)}M, done"
        return :end
    end
    
    ##
    # Loads file.
    #
    
    def load_file(message)
        
        # Stores the metadata
        @meta = message
        
        # Informs
        puts ">> #{@meta.path}"  
        
        # Generates hashes
        Thread::new do
            begin     
                position = 0
                data = true
                
                while data
                    self.file do |file|
                        file.seek(position)

                        # creates local hash table
                        data = file.read(1024 * 1024)
                    end
                    
                    if data
                        @local_hashes << Digest::SHA1.hexdigest(data)
                        position += 1024 * 1024
                    end
                end
                
                # indicates end of stream
                @local_hashes << :end
                
            rescue Exception => e
                puts "Exception: " + e.message + "\n" + e.backtrace.join("\n")
            end
        end
        
    end
    
    ##
    # Adds received hash to remote hashes collection.
    #
    
    def add_hash(message)
        if message.end
            @remote_hashes << :end
        else
            @remote_hashes << message[:hash]
        end
    end
    
    ##
    # Dispatches blocks with both hashes and sends orders back.
    #
    
    def dispatch_orders!
        Thread::new do
            sequence = 0
            remote_end = false
            local_end = false
            
            loop do
                
                # compares each received hashes pair and eventually 
                # orders it 
                remote = @remote_hashes.pop if not remote_end
                local = @local_hashes.pop if not local_end
                
                if remote == :end 
                    remote_end = true
                end
                
                if local == :end 
                    local_end = true
                end

                if local != remote
                    @io.puts MultiJson::dump({
                        :type => :order,
                        :sequence => sequence
                    })
                else
                    @file_bytes += 1024 * 1024
                end
        
                # tracks already processed sequences
                sequence += 1
                remote = nil
                local = nil
                
                # stops processing if everything was ordered
                if remote_end and local_end
                    @io.puts MultiJson::dump({
                        :type => :order,
                        :end => true
                    })
                                      
                    break
                end
                
            end
        end
    end
    
    ##
    # Returns the taget file IO.
    #
    
    def file
        if @file.nil?
        
            # creates the lock object
            @file_lock = Mutex::new
            
            # eventually creates the file
            FileUtils.touch(@meta.path)
            
            # truncates the file according to source file
            if File.size(@meta.path) > @meta[:size]
                File.truncate(@meta.path, @meta[:size])
            end
            
            # opens the file
            @file = File.open(@meta.path, "r+")
            
        end
        
        @file_lock.synchronize do
            yield @file
        end
    end

end

# Main server loop
server = TCPServer.open(7835)
loop do
    Thread.start(server.accept) do |client|
        begin
            handler = Worker::new(client)
            handler.handle!
        rescue Exception => e
            handler.io.puts(e.message)
            handler.io.puts("=> Unidentified problem.")
            handler.close!
        end
    end
end
