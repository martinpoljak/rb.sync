# encoding: utf-8
# (c) 2012 Martin Kozák (martinkozak@martinkozak.net)

require "thread"
require "rb.sync/common/message"

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
        # Constructor.
        #
        # @param [RbSync::IO] io  communication object
        # @param [Logger] logger  logging object
        #
        
        def initialize(io)
            @io = io
        end
                
        ##
        # Negotiates the file transmission.
        #
        # @param [String] from  indicate the source file
        # @param [String] to  indicate the target file
        #
        
        def negotiate_transmission(from, to)
            @io.acquire :write do |io| 
                self.logger.info { "Negotiating." }
                io.puts RbSync::Message::new({
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
                io.puts RbSync::Message::new({
                    :type => :hash,
                    :hash => hash
                })
            end
        end
        
        ##
        # Indicates end.
        # @param [String] hash
        #
        
        def end
            @io.acquire :write do |io|
                io.puts RbSync::Message::new({
                    :type => :end
                })
            end
        end
        
        ##
        # Waits for interaction.
        # @yield [Object] block or message
        #
        
        def wait_interaction
            @io.acquire :read do |io|
                @logger.debug { "Waiting for messages." }
                data = io.read(2)
            end
            
            if data.nil?
                return nil
            else
                version, type, compression = data.unpack('LCC')
                case type
                    when RbSync::Message::type
                        return RbSync::Message::load(@io)
                    #when RbSync::Message::type
                        #RbSync::Message::load(@io_locks, @io)
                end
            end
        end
    
    end
    
end