# encoding: utf-8
# (c) 2012 Martin Kozák (martinkozak@martinkozak.net)

require "thread"
require "rb.sync/common/protocol"

module RbSync
    class Protocol
        module Item

            ##
            # Loads the item content.
            #
            # @param [RbSync::IO] io  input object
            # @return [RbSync::Protocol::Item]
            #
            
            def self.load(io)
                self
            end

            ##
            # Indicates item kind.
            # @return [Integer] kind of the item
            #
            
            def kind
                self.class::kind
            end
            
            ##
            # Serializes the item to string form.
            # @return [String] a string representation
            #
            
            def serialize
                ''
            end
            
            ##
            # Converts to string, so prepends serialization header.
            # @return [String] full item string representation
            #
            
            def to_s
                header = [1, self.kind, 0].pack("LCC")  # protocol version, type, compression type
                self.serialize.prepend(header)
            end

        end
    end
end
