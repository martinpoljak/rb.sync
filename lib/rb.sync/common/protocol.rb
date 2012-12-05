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
        # @param [Hash] options  client settings
        # @return [RbSync::Protocol::Message]
        #
        
        def negotiate(from, to, options)
            message = nil
            
            @io.acquire :write do |io|
                message = RbSync::Protocol::Message::new({
                    :type => :file,
                    :size => File.size(from),
                    :path => to,
                    :blocksize => options.blocksize,
                    :blockcount => options.blockcount,
                    :offset => options.offset
                })
                
                io.write(message)
            end
            
            return message
        end
        
        ##
        # Pushes hash to the server.
        # @param [String] hash
        #
        
        def push_hash(hash)
            @io.acquire :write do |io|
                @logger.debug { "Sending hash of block #{hash}." }
                io.write RbSync::Protocol::Message::new({
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
                @logger.debug { "Indicates termination." }
                io.write RbSync::Protocol::Message::new({
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
  #p data
            if data.nil?
                return nil
            else
                #p "xxxx" + data
                version, type, compression = data.unpack('LCC')
                #p "xxxx" + [version, type, compression].to_s + RbSync::Protocol::Message::kind.to_s
                case type
                    when RbSync::Protocol::Message::kind
                        return RbSync::Protocol::Message::load(@io)
                    when RbSync::Protocol::Block::kind
                        return RbSync::Protocol::Block::load(@io)
                    else
                        @logger.debug { "Really strange data item of type #{type}." }
                        return nil
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
            RbSync::Protocol::Block::new(options).local_to_remote!
        end
        
        ##
        # Orders block of the given sequence.
        # @param [Integer] number  the sequence number
        #
        
        def order_block(number)
            @io.acquire :write do |io|
                io.write RbSync::Protocol::Message::new({
                    :type => :order,
                    :sequence => number
                })
            end
        end
        
        ##
        # Indicates end of ordering.
        #
        
        def end_ordering!
            @io.acquire :write do |io|
                @logger.debug { "Announcing, everything has been ordered." }
                
                io.write RbSync::Protocol::Message::new({
                    :type => :order,
                    :end => true
                })
            end
        end
        
    
    end
    
end
