# encoding: utf-8
require "socket"
require "multi_json"
require "digest/sha1"
require "hashie/mash"
require "xz"
require "thread"
require "logger"

module RbSync
    class Client
        
        ##
        # Remote IO, so target of the transfer.
        # @var IO
        #
        
        @io
        
        ##
        # Remote IO lock.
        # @var Mutex
        #
        
        @io_locks
        
        ##
        # Input for the transfer.
        # @var IO
        #
        
        @file
        
        ##
        # File IO lock.
        # @var Mutex
        #
        
        @file_lock
        
        ##
        # Indicates transfered size of the file.
        # @var Integer
        #
        
        @file_bytes
        
        ##
        # Holds the logger instance.
        # @var Logger
        #
        
        @logger
        
        ##
        # Outcoming hashes queue.
        #
        
        @hash_queue
        
        ##
        # Constructor.
        #
        
        def initialize
            @file_bytes = 0
            @hash_queue = Queue::new
        end
        
        ##
        # Returns the logger instance.
        # @return logger
        #
        
        def logger
            if @logger.nil?
                @logger = Logger::new(STDOUT)
                
                # formatter
                default = Logger::Formatter::new
                @logger.formatter = proc do |severity, datetime, progname, msg|
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
        # @yield IO  the mutexed IO object
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
                self.logger.debug($path) { "Locking remote IO for #{method}." }
                yield @io
                self.logger.debug($path) { "Unlocking remote IO for #{method}." }
            end
        end
        
        ##
        # Returns the source file IO object.
        # @yield IO  the mutexed IO object
        #
        
        def file
            if @file.nil?
                @file_lock = Mutex::new
                @file = File.open($path, 'r')
                self.logger.debug($path) { "Opening for reading." }
            end
            
            @file_lock.synchronize do
                self.logger.debug($path) { "Locking file." }
                yield @file
                self.logger.debug($path) { "Unlocking file." }
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
            
            # dispatches messages
            self.handle_messages!
            
        end
        
        ##
        # Sends initial negotiation.
        #
        
        def negotiate!
        
            # sends initial file metadata
            self.io :write do |io|
                self.logger.info($path) { "Negotiating." }
                
                io.puts MultiJson::dump({
                    :type => :file,
                    :path => $path + ".new",
                    :size => File.size($path)
                })
            end
            
        end
        
        ##
        # Dispatches hashing the file.
        #
        
        def dispatch_hashing!
            Thread::new do
                self.logger.debug($path) { "Starting hashset dispatcher." }
                
                data = true
                position = 0
                
                self.logger.info($path) { "Starting indexing for transfer." }
                
                while data
                    self.file do |file|
                        self.logger.debug($path) { "Reading block from position #{position}." }
                        file.seek(position)
                        data = file.read($blocksize)
                    end
                    
                    position += $blocksize
                    
                    if data
                        @hash_queue << Digest::SHA1.hexdigest(data)
                    end
                end
                
                self.logger.info($path) { "Indexing for transfer finished." }
            end
        end
        
        ##
        # Dispatches pushing hashset to the server.
        #
        
        def dispatch_hashset!
            Thread::new do
                self.logger.debug($path) { "Starting hashing dispatcher." }
                            
                loop do
                    hash = @hash_queue.pop
                    
                    self.io :write do |io|
                        self.logger.debug($path) { "Sending hash of block #{hash}." }
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
            self.logger.debug($path) { "Starting message handler." }
            
            loop do
                data = nil
                
                # reads data
                self.io :read do |io|
                    self.logger.debug($path) { "Waiting for messages." }
                    data = io.gets
                end
                
                message = Hashie::Mash::new(MultiJson::load(data))
                self.logger.debug($path) { "Message of type '#{message.type}' received." }

                # calls processing method according to incoming message
                case message.type.to_sym
                    when :order
                        self.handle_order(message)
                end
                
            end
            
            self.logger.debug($path) { "Message handler terminated." }
        end
        
        ##
        # Handles single order.
        #
        
        def handle_order(message)
        
            self.logger.debug($path) { "Order received for block #{message.sequence}." }
           
            # loads block
            data = nil
            position = message.sequence * $blocksize
            
            self.file do |file|
                self.logger.debug($path) { "Reading block number #{message.sequence} from #{position}." }
                file.seek(position)
                data = file.read($blocksize)
            end
            
            # if something has been loaded, sends it
            if not data.nil?
                #compressed = Zlib::Deflate::deflate(data, Zlib::BEST_COMPRESSION)
                
                self.logger.debug($path) { "Compressing block number #{message.sequence}." }
                compressed = XZ::compress(data)
                self.logger.debug($path) { "Block compressed to size #{compressed.length} (#{((compressed.length / data.length.to_f) * 100).to_i}%)." }
                
                self.io :write do |io|
                    self.logger.debug($path) { "Sending block number #{message.sequence}." }
                    io.write "block" + [message.sequence, compressed.length].pack("QQ")
                    io.write compressed
                end
                
                @file_bytes += data.length
                puts "#{@file_bytes / ($blocksize)}M"
            end
            
        end

    end
end

$blocksize = 1024 * 1024
#$path = 'archive--rclick--20121106.tar'
$path = 'test.dat'

RbSync::Client::new.dispatch!

=begin

        # Terminates
        io.puts MultiJson::dump({
            :type => :end
        })

        io.close()
        file.close()
=end
