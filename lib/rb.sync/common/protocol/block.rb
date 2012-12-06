# encoding: utf-8
# (c) 2012 Martin Kozák (martinkozak@martinkozak.net)

require "multi_json"
require "hashie/mash"
require "hash-utils"
require "rb.sync/common/protocol/item"

module RbSync
    class Protocol

        ##
        # Single message, so protocol metastructure.
        #

        class Block
            include RbSync::Protocol::Item
            
            ##
            # Holds remote input object.
            # @var [RbSync::IO]
            #
            
            @remote_io
            
            ##
            # Holds local input object.
            # @var [RbSync::IO]
            #
            
            attr_writer :local_io
            @local_io
            
            ##
            # Indicates position in local IO.
            # @var [Integer]
            #

            attr_accessor :local_position               
            @local_position
            
            ##
            # Indicates block size in local IO.
            # @var [Integer]
            #
            
            attr_reader :local_size   
            @local_size

            ##
            # Indicates block number in local IO.
            # @var [Integer]
            #
            
            attr_reader :local_number
            @local_number
            
            ##
            # Constructor.
            #
            # @param [Hash] options
            # @option options [RbSync::IO] :remote  remote IO object
            # @option options [RbSync::IO] :local  local IO object
            # @option options [Integer] :local_size  size of the block in local IO
            # @option options [Integer] :local_position  position of the block in local IO
            # @option options [Integer] :local_number  block number in local IO
            # @option options [String] :content  content of the block
            #
            
            def initialize(options = { })
                @local_io = options[:local_io]
                @local_size = options[:local_size]
                @local_position = options[:local_position]
                @local_number = options[:local_number]
                @remote_io = options[:remote_io]
                @content = options[:content]
            end

            ##
            # Loads the message content.
            #
            # @param [RbSync::IO] io  input object
            # @return [RbSync::Protocol::Message]
            #
            
            def self.load(io)
                local_size, local_position, local_number = nil
                content = nil
                
                io.acquire :read do |io|
                    data = io.read(24)
#p data.unpack('QQQ')
                    local_size, local_position, local_number = data.unpack('QQQ')
                    content = io.read(local_size)
                end
                
                self::new(
                    :remote_io => io,
                    :local_size => local_size,
                    :local_number => local_number,
                    :local_position => local_position,
                    :content => content
                )
            end

            ##
            # Indicates item kind (static).
            # @return [Integer] type of the item
            #
            
            def self.kind
                2
            end
            
            ##
            # Serializes block to string. Should be noted, it 
            # serializes header only, not body of the block. 
            # Use {#local_to_remote!}.
            #
            # @return [String]
            #
            
            def serialize
                [@local_size, @local_position, @local_number].pack('QQQ')
            end
            
            ##
            # Copies content of the remote stream to local stream.
            #
            
            def remote_to_local!
                if not @remote_io or not @local_io
                    raise Exception::new("Copy request from remote to local stream, but no local and/or remote stream have been assigned.")
                end
            
                @local_io.acquire do |lio|
                    lio.seek(@local_position)
                    lio.write(@content)
                end
            end

            ##
            # Copies content of the local stream to remote stream.
            #
            
            def local_to_remote!
                if not @remote_io or not @local_io
                    raise Exception::new("Copy request from local to remote stream, but no local and/or remote stream have been assigned.")
                end

                @local_io.acquire do |lio|
                    lio.seek(@local_position)
                    @remote_io.acquire :write do |rio|
  #p rio
                        rio.write(self.to_s)
                        File.copy_stream(lio, rio, @local_size)
                    end
                end

            end
                     
        end
        
    end
end
