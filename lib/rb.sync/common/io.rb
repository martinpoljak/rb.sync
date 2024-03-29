# encoding: utf-8
# (c) 2012 Martin Kozák (martinkozak@martinkozak.net)

require "hash-utils"

##
# IO wrapper.
#

module RbSync
    class IO
        
        ##
        # Contains the parent IO object.
        # @var [IO]
        #
        
        attr_reader :io
        @io
        
        ##
        # Contains locks. Handles 
        # @var [Array, Hash]
        #
        
        @locks
        
        ##
        # Contains logger instance.
        # @var [Logger]
        #
        
        @logger
        
        ##
        # Contains IO type information.
        # @var [Symbol]
        #
        
        @type

        ##
        # Constructor.
        #
        # @param [::IO] io  parent IO file
        # @param [Logger] logger  logging object
        # @param [Symbol] type  type of the IO for reporting
        # @param [Array] locks  locks names list (as symbols) 
        #
        
        def initialize(io, type, logger, locks = [:all])
            @io = io
            @logger = logger
            @type = type
            @locks = locks
        end 
=begin        
        ##
        # Reads data from IO.
        # @param [Integer] length
        #
        
        def read(length)
            @io.read(length)
        end
        
        ##
        # Writes data to IO.
        # @param [String] data  
        #
        
        def write(data)
            @io.write(data)
        end
        
        ##
        # Seeks in the stream.
        # @param [Integer] point  absolute point from beginning
        #
        
        def seek(point)
            @io.seek(point)
        end
        
        ##
        # Returns position from beginning of the stream.
        # @return [Integer]
        #
        
        def pos
            @io.pos
        end
=end        
        ##
        # Acquires given lock.
        # @param [Symbol] name  lock name
        # @yield [IO]
        #
        
        def acquire(name = :all)
            name = name.to_sym
            
            if @locks.array?
                objects = { }
                @locks.each do |name|
                    objects[name] = Mutex::new
                end
                
                @locks = objects
            end
              
            if name.in? @locks
                @logger.debug { "Waiting for #{@type} IO lock for #{name}." }
                @locks[name].synchronize do
                    @logger.debug { "Locking #{@type} IO for #{name}." }
                    yield @io
                    @logger.debug { "Unlocking #{@type} IO for #{name}." }
                end
            else
                yield @io
            end
        end
        
    end
end
