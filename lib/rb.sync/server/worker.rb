# encoding: utf-8
# (c) 2012 Martin Kozák (martinkozak@martinkozak.net)

require "thread"
require "multi_json"
require "hashie/mash"
require "fileutils"
require "digest/sha1"
require "hash-utils"
require "xz"
require "logger"

require "rb.sync/common/io"
require "rb.sync/common/protocol/block"
require "rb.sync/common/protocol/message"
require "rb.sync/common/protocol"

module RbSync
    module Server
        class Worker

            ##
            # Contains the remote IO object.
            # @var [RbSync::IO]
            #
            
            @io
            
            ##
            # Contains teh local IO object.
            # @var [RbSync::IO]
            #
            
            @file
            
            ##
            # Contains the protocol wrapper.
            # @var [RbSync::Protocol]
            #
            
            @protocol
            
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
                
                # Handles input data
                self.handle_data!
                
            end
            
            ##
            # Handles data.
            #
            
            def handle_data!
                self.logger.debug { "Starting data dispatcher." }
                
                loop do
                    data = self.protocol.wait_interaction!
        #p data.inspect
                    # message (process it by message handler)
                    if data.kind_of? RbSync::Protocol::Message
                        result = self.handle_message(data)
                        return if result == :end
                        
                    # block (process it by block handler)
                    elsif data.kind_of? RbSync::Protocol::Block
                        self.handle_block(data)
                    
                    # closed connection (terminate)
                    else
                        self.logger.info { "Connection closed? Termination." }
                        self.terminate!
                    end
                end

            end
            
            ##
            # Handles received block.
            # @param [RbSync::Protocol::Block] block
            #
            
            def handle_block(block)
              
                # analyses metadata
                self.logger.debug { "Block #{block.local_number} with compressed size #{block.local_size} received." }

                # reads data
                #data = nil
                #self.logger.debug { "Reading the data of block #{sequence}." }
                #self.io.acquire :read do |io|
                #    self.logger.debug { "Reading the data of block #{sequence}." }
                #    data = io.read(size)
                #end
                
                #data = Zlib::Inflate::inflate(data)
                #self.logger.debug { "Decompressing block #{sequence}." }
                #data = XZ::decompress(data)
                
                #self.file.acquire do |file|
                #    self.logger.debug { "Writing #{data.length} bytes of block #{sequence}." }
                #    file.seek(sequence * 1024 * 1024)
                #    file.write(data)
                #end
                
          #self.logger.debug { "x" * 1000 }
          
                # if cutting, positions for writing are relative to offset
                if @meta.cut
                    block.local_position -= @meta.offset * @meta.blocksize
                end
                
                # writes
                block.local_io = self.file
                self.logger.debug { "Writing #{block.local_size} bytes of block #{block.local_number} to position #{block.local_position}." }
                block.remote_to_local!
                
                @file_bytes += block.local_size
                self.report!
            end
            
            ##
            # Handles message.
            #
            # @param [RbSync::Protocol::Message] message
            # @return [nil, Symbol] +nil+ or +:end+ if it's 
            #   terminating message 
            #
            
            def handle_message(message)
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
                
                self.io.acquire :read do
                    self.io.acquire :write do |io|
                        self.logger.info { "Closing remote IO." }
                        io.close()
                    end
                end
                
                self.file.acquire do |file|
                    self.logger.info { "Closing file." }
                    file.close()
                end
                
                puts "#{@file_bytes / (1024 * 1024)}M, done"
                return :end
            end
            
            ##
            # Loads file.
            # @param [RbSync::Protocol::Message] message
            #
            
            def load_file(message)
                
                # Stores the metadata
                @meta = message
                
                # Informs
                self.logger.info { "Starting processing of file with size #{@meta[:size]}." }
                
                # Starts dispatching orders
                self.dispatch_orders!
                
                # Generates hashes
                Thread::new do
                    begin  
                        data = true
                                                
                        if @meta.cut
                            position = 0
                        else
                            position = @meta.offset * @meta.blocksize
                        end
                        
                        if @meta.blockcount
                            target = position + @meta.blockcount * @meta.blocksize
                        else
                            target = nil
                        end
                        
                        ###
                        
                        self.logger.debug { "Starting indexing." }
                        
                        while data and (target.nil? or position < target)
                            self.file.acquire do |file|
                                file.seek(position)

                                # creates local hash table
                                self.logger.debug { "Reading block starting at #{position}." }
                                data = file.read(@meta.blocksize)
                            end
                            
                            if data
                                self.logger.debug { "Generating hash for block starting at #{position}." }
                                hash = Digest::SHA1.hexdigest(data)
                                @local_hashes << hash
                                self.logger.debug { "Adding local hash #{hash}." }
                                position += @meta.blocksize
                            end
                        end
                        
                        # indicates end of stream
                        self.logger.debug { "Adding local hash #{:end}." }
                        self.logger.debug { "Indexing finished." }
                        @local_hashes << :end
                        
                    rescue Exception => e
                        self.logger.fatal { "#{e.class.name}: #{e.message}\n #{e.backtrace.join("\n")}" }
                    end
                end
                
            end
            
            ##
            # Adds received hash to remote hashes collection.
            # @param [RbSync::Protocol::Message] message
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
                    begin
                        self.logger.debug { "Starting orders dispatcher." }
                        
                        sequence = @meta.offset
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
                                local = nil
                            end
                            
                            # remote has data, but local not or aren't matching
                            if not remote_end and (local_end or (local != remote))
                                self.logger.debug { "Ordering block #{sequence}." }
                                self.protocol.order_block(sequence)
                            # both remote and local has data but are matching
                            elsif not remote_end
                                self.logger.debug { "Block #{sequence} is matching." }
                                @file_bytes += @meta.blocksize
                            # remote hasn't data, which means also local end
                            else
                                self.logger.debug { "No more data in remote means ordering is finished." }
                                local_end = true
                            end
                            
                            # stops processing if everything was ordered                            
                            if remote_end and local_end
                                self.protocol.end_ordering!
                                break
                            end
                    
                            # tracks already processed sequences
                            sequence += 1
                            
                        end
                        
                    rescue Exception => e
                        self.logger.fatal { "#{e.class.name}: #{e.message}\n #{e.backtrace.join("\n")}" }
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
                        self.logger.debug { "Truncating file to size #{@meta[:size]}." }
                        File.truncate(@meta.path, @meta[:size])
                    end
                    
                    # opens the file
                    self.logger.debug { "Opening file." }
                    file = File.open(@meta.path, "r+")
                    @file = RbSync::IO::new(file, :file, self.logger)
                    
                end
                
                return @file
            end
            
            ##
            # Returns the client connection IO object.
            # @return [RbSync::IO]]  IO object
            #
            
            def io
                if @io.kind_of? ::IO
                    @io = RbSync::IO::new(@io, :remote, self.logger, [:read, :write])
                else
                    @io
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

            ##
            # Returns the protocol object.
            # @return [RbSync::Protocol]
            #
            
            def protocol
                if @protocol.nil?
                    @protocol = RbSync::Protocol::new(self.io, self.logger)
                else
                    @protocol
                end
            end
            
        end
    end
end
