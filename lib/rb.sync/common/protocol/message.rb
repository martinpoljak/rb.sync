# encoding: utf-8
# (c) 2012 Martin Kozák (martinkozak@martinkozak.net)

require "multi_json"
require "hashie/mash"
require "rb.sync/common/protocol/item"

module RbSync
    class Protocol

        ##
        # Single message, so protocol metastructure.
        #

        class Message < Hashie::Mash
            include RbSync::Protocol::Item

            ##
            # Loads the message content.
            #
            # @param [RbSync::IO] io  input object
            # @return [RbSync::Protocol::Message]
            #
            
            def self.load(io)
            
                # Reads the data
                data = nil
                io.acquire :read do
                    size = io.read(8)
                    data = io.read(size)
                end
                
                # Analyzes the data
                msg = MultiJson::load(data)
                return self::new(msg)
                
            end

            ##
            # Indicates item kind (static).
            # @return [Integer] type of the item
            #
            
            def self.kind
                1
            end

            ##
            # Indicates item kind.
            # @return [Integer] kind of the item
            #
            
            def kind
                self.class::kind
            end
            
            ##
            # Serializes message to string.
            # @return [String]
            #
            
            def serialize
                data = MultiJson::dump(self)
                header = [data.length].pack('Q')
                data.prepend(header)
            end
                     
        end
        
    end
end
