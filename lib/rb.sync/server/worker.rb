# (c) 2012 Martin Kozák (martinkozak@martinkozak.net)
# encoding: utf-8

require "thread"
require "multi_json"
require "hashie/mash"
require "fileutils"
require "digest/sha1"
require "hash-utils"
require "xz"
require "logger"

module RbSync
    module Server
        class Worker

            @io
            @io_locks
            
            @file
            @file_lock
            
            @local_hashes
            @remote_hashes
            @file_bytes
            
            @logger
            
            ##
            # Constructor.
            #
            
            def initialize(io)
                @io = io
                @file_bytes = 0
                @remote_hashes = Queue::new
                @local_hashes = Queue::new
                
                @io_locks = {
                    :read => Mutex::new,
                    :write => Mutex::new
                }
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
                
                self.logger.info { "Starting worker." }
                
                # Starts dispatching orders
                self.dispatch_orders!
                
                # Handles input data
                self.handle_data!
                
            end
            
            ##
            # Handles data.
            #
            
            def handle_data!
                self.logger.debug { "Starting data dispatcher." }
                
                loop do
                    prefix = nil
                    
                    self.io :read do |io|
                        self.logger.debug { "Waiting for data." }
                        prefix = io.read(5)
                        self.logger.debug { "Data arrived." }
                    end

                    # nil received, it's reason for termination
                    if prefix.nil?
                        self.logger.info { "Connection closed? Termination." }
                        self.terminate!
                        
                    # block detected
                    elsif prefix == "block"
                        meta = nil
                        self.io :read do |io|
                            self.logger.debug { "Reading block." }
                            meta = @io.read(16)
                        end
                        
                        self.handle_block(meta, @io)
                        
                    # in otherwise, it's message
                    else
                        data = nil
                        self.io :read do |io|
                            self.logger.debug { "Reading message." }
                            data = io.gets
                        end
                        
                        result = self.handle_message(prefix + data)
                        return if result == :end
                        
                    end
                end
            end
            
            ##
            # Handles received block.
            #
            
            def handle_block(meta, io)
              
                # analyses metadata
                sequence, size = meta.unpack("QQ")
                self.logger.debug { "Block #{sequence} with compressed size #{size} received." }

                # reads data
                data = nil
                self.io :read do |io|
                    self.logger.debug { "Reading the data of block #{sequence}." }
                    data = io.read(size)
                end
                
                #data = Zlib::Inflate::inflate(data)
                self.logger.debug { "Decompressing block #{sequence}." }
                #data = XZ::decompress(data)
                
                self.file do |file|
                    self.logger.debug { "Writing #{data.length} bytes of block #{sequence}." }
                    file.seek(sequence * 1024 * 1024)
                    file.write(data)
                end
                
                @file_bytes += data.length
                self.report!
            end
            
            ##
            # Handles message.
            #
            
            def handle_message(data)
                message = Hashie::Mash::new(MultiJson::load(data))
                self.logger.debug { "Message of type '#{message.type}' received." }

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
                self.logger.info { "Terminating worker." }
                
                self.io :read do
                    self.io :write do |io|
                        self.logger.info { "Closing remote IO." }
                        io.close()
                    end
                end
                
                self.file do |file|
                    self.logger.info { "Closing file." }
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
                self.logger.info { "Starting processing of file with size #{@meta.size}." }
                
                # Generates hashes
                Thread::new do
                    begin     
                        position = 0
                        data = true
                        
                        self.logger.debug { "Starting indexing." }
                        
                        while data
                            self.file do |file|
                                file.seek(position)

                                # creates local hash table
                                self.logger.debug { "Reading block starting at #{position}." }
                                data = file.read(1024 * 1024)
                            end
                            
                            if data
                                self.logger.debug { "Generating hash for block starting at #{position}." }
                                hash = Digest::SHA1.hexdigest(data)
                                @local_hashes << hash
                                self.logger.debug { "Adding local hash #{hash}." }
                                position += 1024 * 1024
                            end
                        end
                        
                        # indicates end of stream
                        @local_hashes << :end
                        
                    rescue Exception => e
                        self.logger.fatal { "Exception: #{e.message}\n #{e.backtrace.join("\n")}" }
                    end
                end
                
            end
            
            ##
            # Adds received hash to remote hashes collection.
            #
            
            def add_hash(message)
                if message.end
                    self.logger.debug { "All remote hashes received." }
                    @remote_hashes << :end
                else
                    self.logger.debug { "Adding remote hash #{message[:hash]}." }
                    @remote_hashes << message[:hash]
                end
            end
            
            ##
            # Dispatches blocks with both hashes and sends orders back.
            #
            
            def dispatch_orders!
                Thread::new do
                    self.logger.debug { "Starting orders dispatcher." }
                    
                    sequence = 0
                    remote_end = false
                    local_end = false
                    
                    loop do
                        
                        # compares each received hashes pair and eventually 
                        # orders it 
                        remote = @remote_hashes.pop if not remote_end
                        local = @local_hashes.pop if not local_end
                        
                        if remote.to_sym == :end 
                            remote_end = true
                        end
                        
                        if local.to_sym == :end 
                            local_end = true
                        end

                        if local.to_sym != :end and remote.to_sym != :end and local != remote
                            self.io :write do |io|
                                self.logger.debug { "Ordering block #{sequence}." }
                                
                                io.puts MultiJson::dump({
                                    :type => :order,
                                    :sequence => sequence
                                })
                            end
                        else
                            self.logger.debug { "Block #{sequence} is matching." }
                            @file_bytes += 1024 * 1024
                        end
                
                        # tracks already processed sequences
                        sequence += 1
                        remote = nil
                        local = nil
                        
                        # stops processing if everything was ordered
                        
                        if remote_end and local_end
                            self.io :write do |io|
                                self.logger.debug { "Announcing, everything has been ordered." }
                                
                                io.puts MultiJson::dump({
                                    :type => :order,
                                    :end => true
                                })
                            end
                                              
                            break
                        end
                        
                    end
                end
            end
            
            ##
            # Yields the target file IO.
            # @yield IO
            #
            
            def file
                if @file.nil?
                
                    # creates the lock object
                    @file_lock = Mutex::new
                    
                    # eventually creates the file
                    if not File.exist? @meta.path
                        self.logger.debug { "Creating the file." }
                        FileUtils.touch(@meta.path)
                    end
                    
                    # truncates the file according to source file
                    if File.size(@meta.path) > @meta[:size]
                        self.logger.debug { "Truncating file to size #{meta[:size]}." }
                        File.truncate(@meta.path, @meta[:size])
                    end
                    
                    # opens the file
                    self.logger.debug { "Opening file." }
                    @file = File.open(@meta.path, "r+")
                    
                end
                
                @file_lock.synchronize do
                    self.logger.debug { "Locking file." }
                    yield @file
                    self.logger.debug { "Unlocking file." }
                end
            end
            
            ##
            # Returns the client connection IO object.
            #
            # @param [:read, :write] method  method for correct locking
            # @yield IO  the mutexed IO object
            #
            
            def io(method)
                @io_locks[method].synchronize do
                    self.logger.debug { "Locking remote IO for #{method}." }
                    yield @io
                    self.logger.debug { "Unlocking remote IO for #{method}." }
                end
            end
                
            ##
            # Returns the logger instance.
            # @return Logger
            #
            
            def logger
                if @logger.nil?
                    @logger = Logger::new(STDOUT)
                    
                    # formatter
                    default = Logger::Formatter::new
                    @logger.formatter = proc do |severity, datetime, progname, msg|
                        if @meta and @meta.path?
                            progname = @meta.path
                        end
                    
                        msg = "##{Thread.current.object_id} " + msg 
                        default.call(severity, datetime, progname, msg)
                    end
                end
                
                return @logger
            end

        end
    end
end
