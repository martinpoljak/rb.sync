# encoding: utf-8
# (c) 2012 Martin Kozák (martinkozak@martinkozak.net)

require "socket"
require "multi_json"
require "digest/sha1"
require "hashie/mash"
require "xz"
require "thread"
require "logger"
require "trollop"

require "rb.sync/common/protocol"
require "rb.sync/common/io"
require "rb.sync/common/protocol/block"

module RbSync
    class Client
        
        ##
        # Remote IO, so target of the transfer.
        # @var [IO]
        #
        
        @io
        
        ##
        # Protocol handler.
        # @var [RbSync::Protocol]
        #
        
        @protocol
        
        ##
        # Input for the transfer.
        # @var [RbSync::IO]
        #
        
        @file
        
        ##
        # Indicates transfered size of the file.
        # @var [Integer]
        #
        
        @file_bytes
        
        ##
        # Holds the logger instance.
        # @var [Logger]
        #
        
        @logger
        
        ##
        # Outcoming hashes queue.
        # @var [Queue]
        #
        
        @hash_queue
        
        ##
        # Placed orders queue.
        # @var [Queue]
        #
        
        @orders_queue
        
        ##
        # Transfer settings.
        # @var [Hash]
        #
        
        @options
        
        ##
        # Targets settings.
        # @var [Array]
        #
        
        @targets
        
        ##
        # Negotiated metainformations.
        # @var [Hash]
        #
        
        @meta
        
        ##
        # Constructor.
        # @param [Hash] options  client settings
        # @param [Class] targets  files for copy
        #
        
        def initialize(options, targets)
            @options = options
            @targets = targets
            @file_bytes = 0
            @hash_queue = Queue::new
            @orders_queue = Queue::new
        end
        
        ##
        # Returns the logger instance.
        # @return [Logger]
        #
        
        def logger
            if @logger.nil?
                @logger = Logger::new(STDOUT)
                
                # formatter
                default = Logger::Formatter::new
                @logger.formatter = proc do |severity, datetime, progname, msg|
                    progname = @targets.from
                    msg = "##{Thread.current.object_id} " + msg 
                    default.call(severity, datetime, progname, msg)
                end
            end
            
            return @logger
        end
        
        ##
        # Returns the server connection IO object.
        # @return [RbSync::IO]  the mutexable IO object
        #
        
        def io
            if @io.nil?
                self.logger.info("Connecting.")
                io = TCPSocket.new 'localhost', 7835 #110
                @io = RbSync::IO::new(io, :remote, self.logger, [:read, :write])
            else
                @io
            end
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
        
        ##
        # Returns the source file IO object.
        # @yield [IO]  the mutexed IO object
        #
        
        def file
            if @file.nil?
                self.logger.debug { "Opening for reading." }
                file = File.open(@targets.from, 'r')
                @file = RbSync::IO::new(file, :file, self.logger)
            else
                @file
            end
        end
        
        ##
        # Dispatches the file transfer.
        #
        
        def dispatch!
        
            # negotiates initial configuration
            @meta = self.negotiate!
            
            # dispatches hash set to the server
            self.dispatch_hashing!
            self.dispatch_hashset!
            
            # dispatches orders
            self.dispatch_orders!
            
            # dispatches messages
            self.handle_messages!
            
        end
        
        ##
        # Sends initial negotiation.
        #
        
        def negotiate!
            self.protocol.negotiate(@targets.from, @targets.to, @options)
        end
        
        ##
        # Dispatches hashing the file.
        #
        
        def dispatch_hashing!
            Thread::new do
                begin
                    self.logger.debug { "Starting hashset dispatcher." }
                    
                    data = true
                    position = 0
                    
                    self.logger.info { "Starting indexing for transfer." }
                    
                    while data
                        self.file.acquire do |file|
                            self.logger.debug { "Reading block from position #{position}." }
                            file.seek(position)
                            data = file.read(@options.blocksize)
                        end
                        
                        position += @options.blocksize
                        
                        if data
                            @hash_queue << Digest::SHA1.hexdigest(data)
                        end
                    end
                    
                    # indicates finish
                    @hash_queue << :end
                    
                    self.logger.info { "Indexing for transfer finished." }
                    
                rescue Exception => e
                    self.logger.fatal { "#{e.class.name}: #{e.message}\n #{e.backtrace.join("\n")}" }
                end
            end
        end
        
        ##
        # Dispatches pushing hashset to the server.
        #
        
        def dispatch_hashset!
            Thread::new do
                begin
                    self.logger.debug { "Starting hashing dispatcher." }     
                    loop do
                        hash = @hash_queue.pop
                        self.protocol.push_hash(hash)
                    end
                
                rescue Exception => e
                    self.logger.fatal { "#{e.class.name}: #{e.message}\n #{e.backtrace.join("\n")}" }
                end
            end
        end
        
        ##
        # Handles all incoming messages.
        #
        
        def handle_messages!
            self.logger.debug { "Starting message handler." }
            
            loop do
                message = nil

                # reads data
                self.logger.debug { "Waiting for messages." }
                message = self.protocol.wait_interaction!
                
                # if nil data arrived, it means termination
                if message.nil?
                    break
                end
                
                self.logger.debug { "Message of type '#{message.type}' received." }

                # calls processing method according to incoming message
                case message.type.to_sym
                    when :order
                        self.handle_order(message)
                end
                
            end
            
            self.logger.debug { "Message handler terminated." }
        end
        
        ##
        # Handles single order.
        # @param [Hashie::Mash] ordering message
        #
        
        def handle_order(message)
            if not message.end
                self.logger.debug { "Order received for block #{message.sequence}." }
            else
                self.logger.debug { "Ordering end indication received." }
            end
            
            @orders_queue << message
        end

        ##
        # Dispatches orders realising.
        #
        
        def dispatch_orders!
            t = Thread::new do
                begin
                    self.logger.debug { "Starting orders dispatcher." }
                    
                    loop do
                        message = @orders_queue.pop
                        
                        # eventually terminates processing if it's finished
                        if message.end and @orders_queue.empty?
                            self.logger.debug { "All orders realised. Terminating." }
                            self.protocol.end!
                            self.terminate!
                            
                            #t.exit
                            #return
                            break
                        else
                            # sends block
                            self.logger.debug { "Sending block number #{message.sequence} of size #{@options.blocksize}." }
                            
                            position = message.sequence * @options.blocksize
                            if @meta[:size] - position < @options.blocksize
                                blocksize = @meta[:size] - position
                            else
                                blocksize = @options.blocksize
                            end
                            #p blocksize
                            self.protocol.send_block(
                                :local_number => message.sequence,
                                :local_position => position,
                                :local_size => blocksize,
                                :local_io => self.file,
                                :remote_io => self.io
                            )
                        end
=begin                    
                        data = nil
                        position = message.sequence * @options.blocksize
                        
                        self.file.acquire do |file|
                            self.logger.debug { "Reading block number #{message.sequence} from #{position}." }
                            file.seek(position)
                            data = file.read(@options.blocksize)
                        end
                        
                        # if something has been loaded, sends it
                        if not data.nil?
                            self.logger.debug { "Compressing block number #{message.sequence}." }
                            #compressed = XZ::compress(data)
                            #compressed = Zlib::Deflate::deflate(data, Zlib::BEST_COMPRESSION)
                            compressed = data
                            self.logger.debug { "Block compressed to size #{compressed.length} (#{((compressed.length / data.length.to_f) * 100).to_i}%)." }
                            
                            self.io :write do |io|
                                self.logger.debug { "Sending block number #{message.sequence}." }
                                io.write "block" 
                                io.write [message.sequence, compressed.length].pack("QQ")
                                io.write compressed
                            end
                            
                            @file_bytes += data.length
                            puts "#{@file_bytes / (@options.blocksize)}M"
                        end
=end                    
                    end
                    
                rescue Exception => e
                    self.logger.fatal { "#{e.class.name}: #{e.message}\n #{e.backtrace.join("\n")}" }
                end
            end
            
        end
        
        ##
        # Terminates the client.
        #
        
        def terminate!
            self.file.acquire do |file|
                self.logger.debug { "Closing the file." }
                file.close()
            end
            
            self.io.acquire :read do
                self.io.acquire :write do |io|
                    self.logger.debug { "Closing the remote IO." }
                    io.close()
                end
            end
        end

    end
end
