# (c) 2012 Martin Kozák (martinkozak@martinkozak.net)
# encoding: utf-8

require "socket"
require "multi_json"
require "digest/sha1"
require "hashie/mash"
require "xz"
require "thread"
require "logger"
require "trollop"

module RbSync
    class Client
        
        ##
        # Remote IO, so target of the transfer.
        # @var [IO]
        #
        
        @io
        
        ##
        # Remote IO lock.
        # @var [Mutex]
        #
        
        @io_locks
        
        ##
        # Input for the transfer.
        # @var [IO]
        #
        
        @file
        
        ##
        # File IO lock.
        # @var [Mutex]
        #
        
        @file_lock
        
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
        #
        # @param [:read, :write] method  method for correct locking
        # @yield [IO]  the mutexed IO object
        #
        
        def io(method)
            if @io.nil?
                @io_locks = {
                    :read => Mutex::new,
                    :write => Mutex::new
                }
                
                @io = TCPSocket.new 'localhost', 7835#110
                self.logger.info("Connecting.")
            end
            
            @io_locks[method].synchronize do
                self.logger.debug { "Locking remote IO for #{method}." }
                yield @io
                self.logger.debug { "Unlocking remote IO for #{method}." }
            end
        end
        
        ##
        # Returns the source file IO object.
        # @yield [IO]  the mutexed IO object
        #
        
        def file
            if @file.nil?
                @file_lock = Mutex::new
                @file = File.open(@targets.from, 'r')
                self.logger.debug { "Opening for reading." }
            end
            
            @file_lock.synchronize do
                self.logger.debug { "Locking file." }
                yield @file
                self.logger.debug { "Unlocking file." }
            end
        end
        
        ##
        # Dispatches the file transfer.
        #
        
        def dispatch!
        
            # negotiates initial configuration
            self.negotiate!
            
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
        
            # sends initial file metadata
            self.io :write do |io|
                self.logger.info { "Negotiating." }
                
                io.puts MultiJson::dump({
                    :type => :file,
                    :path => @targets.to,
                    :size => File.size(@targets.from)
                })
            end
            
        end
        
        ##
        # Dispatches hashing the file.
        #
        
        def dispatch_hashing!
            Thread::new do
                self.logger.debug { "Starting hashset dispatcher." }
                
                data = true
                position = 0
                
                self.logger.info { "Starting indexing for transfer." }
                
                while data
                    self.file do |file|
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
            end
        end
        
        ##
        # Dispatches pushing hashset to the server.
        #
        
        def dispatch_hashset!
            Thread::new do
                self.logger.debug { "Starting hashing dispatcher." }
                            
                loop do
                    hash = @hash_queue.pop
                    
                    self.io :write do |io|
                        self.logger.debug { "Sending hash of block #{hash}." }
                        io.puts MultiJson::dump({
                            :type => :hash,
                            :hash => hash
                        })
                    end
                end
            end
        end
        
        ##
        # Handles all incoming messages.
        #
        
        def handle_messages!
            self.logger.debug { "Starting message handler." }
            
            loop do
                data = nil
                
                # reads data
                self.io :read do |io|
                    self.logger.debug { "Waiting for messages." }
                    data = io.gets
                end
                
                # if nil data arrived, it means termination
                if data.nil?
                    break
                end
                
                message = Hashie::Mash::new(MultiJson::load(data))
                p message
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
            Thread::new do
                self.logger.debug { "Starting orders dispatcher." }
                
                loop do
                    message = @orders_queue.pop
                   
                    # eventually terminates processing it's finished
                    if message.end and @orders_queue.empty?
                        self.logger.debug { "All orders realised. Terminating." }
                        
                        self.io :write do |io|
                            io.puts MultiJson::dump({
                                :type => :end
                            })
                        end
                        
                        self.terminate!
                        return
                    end
                
                    # loads block
                    data = nil
                    position = message.sequence * @options.blocksize
                    
                    self.file do |file|
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
                    
                end
            end
            
        end
        
        ##
        # Terminates the client.
        #
        
        def terminate!
            self.file do |file|
                self.logger.debug { "Closing the file." }
                file.close()
            end
            
            self.io :read do
                self.io :write do |io|
                    self.logger.debug { "Closing the remote IO." }
                    io.close()
                end
            end
        end

    end
end
