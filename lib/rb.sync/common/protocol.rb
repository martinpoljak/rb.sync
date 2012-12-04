# encoding: utf-8
# (c) 2012 Martin Kozák (martinkozak@martinkozak.net)

require "thread"
require "rb.sync/common/protocol/message"
require "rb.sync/common/protocol/block"

module RbSync

    ##
    # Protocol helper class. It's thread safe.
    #

    class Protocol
    
        ##
        # IO object for communicating.
        # @var [RbSync::IO]
        # 
        
        @io
        
        ##
        # Logging object.
        # @var [Logger]
        #
        
        @logger
        
        ##
        # Constructor.
        #
        # @param [RbSync::IO] io  communication object
        # @param [Logger] logger  logging object
        #
        
        def initialize(io, logger)
            @io = io
            @logger = logger
        end
                
        ##
        # Negotiates the file transmission.
        #
        # @param [String] from  indicate the source file
        # @param [String] to  indicate the target file
        #
        
        def negotiate(from, to)
            @io.acquire :write do |io|
                @logger.info { "Negotiating." }
                
                io.puts RbSync::Protocol::Message::new({
                    :type => :file,
                    :size => File.size(from),
                    :path => to,
                })
            end
        end
        
        ##
        # Pushes hash to the server.
        # @param [String] hash
        #
        
        def push_hash(hash)
            @io.acquire :write do |io|
                @logger.debug { "Sending hash of block #{hash}." }
                io.puts RbSync::Protocol::Message::new({
                    :type => :hash,
                    :hash => hash
                })
            end
        end
        
        ##
        # Indicates end.
        #
        
        def end!
            @io.acquire :write do |io|
                io.puts RbSync::Protocol::Message::new({
                    :type => :end
                })
            end
        end
        
        ##
        # Waits for interaction.
        # @yield [Object] block or message
        #
        
        def wait_interaction!
            data = nil
            
            @io.acquire :read do |io|
                @logger.debug { "Waiting for messages." }
                data = io.read(6)
            end
            
            if data.nil?
                return nil
            else
                version, type, compression = data.unpack('LCC')
                case type
                    when RbSync::Protocol::Message::type
                        return RbSync::Protocol::Message::load(@io)
                    when RbSync::Protocol::Block::type
                        return RbSync::Protocol::Block::load(@io)
                end
            end
        end
        
        ##
        # Sends block data.
        #
        # @param [Hash] options
        # @option options [RbSync::IO] :remote  remote IO object
        # @option options [RbSync::IO] :local  local IO object
        # @option options [Integer] :local_size  size of the block in local IO
        # @option options [Integer] :local_position  position of the block in local IO
        # @option options [Integer] :local_number  block number in local IO
        #
        
        def send_block(options)
              block = RbSync::Protocol::Block::new(
                  :local_number => message.sequence,
                  :local_position => message.sequence * @options.blocksize,
                  :local_size => @options.blocksize,
                  :local => self.file,
                  :remote => self.io
              )
              
              block.local_to_remote!
        end
        
    
    end
    
end
