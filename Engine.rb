# The Engine class is used to connect to the Simularity High Performance
# Correlation Engine, and can be used to load data into the engine
#
# Author::      Ray Richardson (mailto:ray@simularity.com)
# Copyright::   Copyright 2011 - 2015 Simularity, Inc.
# License::     MIT License

require 'socket'
require 'net/http/persistent'
require 'json'


class Engine
  private 
  # The Segment class internally manages the segment connection, and the webservice
  # transaction which loads/deletes data
  class Segment
    # Initialize with a host, port and number of items to cache before sending
    def initialize(host, port, range=10000)
      @host = host
      @port = port
      @range = range
      @data = "["
      @count = 0
      @http = Net::HTTP::Persistent.new 'rubyloader'
    end

    # apply a subject/action/object triple
    def apply_subj(st, si, a, ot, oi)
      if @data != "[" then
        @data << ","
      end
      @data << "triple(subject(#{st},#{si}),#{a},object(#{ot},#{oi}))"
      @count = @count + 1
      if @count >= @range then
        flush
      end
    end

    # apply an object/action/subject triple
    def apply_obj(ot, oi, a, st, si)
      if @data != "[" then
        @data << ","
      end
      @data << "triple(object(#{ot},#{oi}), #{a}, subject(#{st},#{si}))"
      @count = @count + 1
      if @count >= @range then
        flush
      end
    end

    # send the data to the segment
    def flush
      @data << "]"
      @count = 0
      uri = URI "http://#{@host}:#{@port}/load_data"
      post = Net::HTTP::Post.new uri.path
      post.body = @data
      @data = "["
      response = @http.request uri, post
      json = JSON.parse(response.body)
      if json["status"] != 0 then
        puts json
      end
    end
  end
  
  public
# These parameters are for the "server" instance of the engine (typically Segment 0)
  def initialize(host="localhost", port=3000)
    @open = false
    open(host, port)
    @obj_names = {}
    @subjects = {}
    @objects = {}
    @actions = {}
  end

  # open connects this instance to the instance of the engine, retrieving the
  # description of the engine's segment structure and initializing the data
  # structures needed to communicate with it. It does not clear the records of names
  # Types, Objects or Actions - these persist throughout the lifetime of this instance
  def open(host, port)
    if @open then
      close
      @open = false
    end
    @host = host
    @port = port

# Connect and get the segments
    url_str = "http://#{host}:#{port}/segments"
    puts url_str
    uri = URI(url_str)
    segstr = Net::HTTP.get(uri)
    segjson = JSON.parse(segstr)
    
    @nsegs = 0
    if segjson["status"] == 0 then
      @segments = Array.new
      @sockets = Array.new
      segments = segjson["segments"]
      segments.each do | seg |
#        @segments << [seg["segment"], seg["host"], seg["port"]]
#        @sockets << TCPSocket.open(seg["host"], seg["port"])
        @segments << Segment.new(seg["host"], seg["sport"])
        @nsegs = @nsegs+1
      end
    else
      raise "Could Not retrieve Segments from server"
    end
    # Get the ItemSize
    begin
      url_str = "http://#{host}:#{port}/itemsize"
      uri = URI(url_str)
      szstr = Net::HTTP.get(uri)
      szjson = JSON.parse(szstr)
      if szjson["status"] == 0 then
        @max_item = szjson["max"]
        @min_item = szjson["min"]
      else
        @max_item = 0xffffffff
        @min_item = 0
      end
    rescue
      @max_item = 0xffffffff
      @min_item = 0
    end
    @scount = 0
    @snilcount = 0
  end

  # Check that an item id is in range for the engine we're connected to 
  def item_check(item)
    if item.is_a?(Integer) then
      if item >= @min_item
        if item <= @max_item
          return true
        end
      end
    end
    return false
  end

  # Check that at type is in range
  def type_check(type)
    if type.is_a?(Integer) then
      if type > 0
        if type <= 0x7fff
          return true
        end
     end
    end
    return false
  end

  # check that an action is in range
  def action_check(action)
    if action.is_a?(Integer) then
      if action > 0 then
        if action <= 0x7f then
          return true;
        end
      end
    end
    return false
  end

  # return a subject type id for a symbol
  def get_stype(symbol)
    if ! symbol then
      return 0
    end

    if symbol.is_a?(Integer) then
      return symbol
    else
      lookup = @subjects[symbol]
      if lookup then
        return lookup
      end
    end

# if we get here we have to go to the server
    uri = URI("http://#{@host}:#{@port}/convert_type?name=#{symbol}&class=subject")
    jsonstr = Net::HTTP.get(uri)
    json = JSON.parse(jsonstr)
    
    if json["status"] == 0 then
      value = json["id"].to_i
      @subjects[symbol] = value
      return value
    else
      return 0
    end
  end


  # return a type id for an object type
  def get_otype(symbol)
    if ! symbol then
      return 0
    end
    
    if symbol.is_a?(Integer) then
      return symbol
    else
      lookup = @objects[symbol]
      if lookup then
        return lookup
      end
    end

# if we get here we have to go to the server
    uri = URI("http://#{@host}:#{@port}/convert_type?name=#{symbol}&class=object")
    jsonstr = Net::HTTP.get(uri)
    json = JSON.parse(jsonstr)
    
    if json["status"] == 0 then
      value = json["id"].to_i
      @objects[symbol] = value
      return value
    else
      return 0
    end
  end

  # return the id for an action
  def get_action(symbol)
    if ! symbol then
      return 0
    end
    if symbol.is_a?(Integer) then
      return symbol
    else
      lookup = @actions[symbol]
      if lookup then
        return lookup
      end
    end

# if we get here we have to go to the server
    url = "http://#{@host}:#{@port}/convert_action?name=#{symbol}"
    uri = URI(url)
    jsonstr = Net::HTTP.get(uri)
    json = JSON.parse(jsonstr)
    
    if json["status"] == 0 then
      value = json["id"].to_i
      @actions[symbol] = value
      return value
    else
      return 0
    end
  end

  # transform a symbol so it contains underscores insted of spaces
  def no_spaces(name)
    if name.is_a?(String) then
      name.gsub(' ', '_')
    else
      name
    end
  end

  # get an object id by type specification
  def get_object(name, typeclass, type)

    if ! name then
      return 0
    end
    if name.is_a?(Integer) then
      result = name
    elsif name.is_a?(Float) then
      result = name
    else
      result = @obj_names[[name, typeclass, type]]
      if ! result then
        esc = URI.escape("http://#{@host}:#{@port}/convert_object?class=#{typeclass}&name=#{name}&type=#{type}")
        uri = URI(esc)
        jsonstr = Net::HTTP.get(uri)
        json = JSON.parse(jsonstr)
        if json["status"] = 0 then
          result = json["id"].to_i
          @obj_names[[name, typeclass, type]] = result
        else
          result = 0
        end
      end
    end
    return result
  end
    
        

  # Add a triple to the connected engine. This routine adds to both the
  # subjective and objective segment
  def apply(stype, sitem, action, otype, oitem)

#    pstype = prologify(stype)
#    psitem = prologify(sitem)
#    paction = prologify(action)
#    potype = prologify(otype)
#    poitem = prologify(oitem)

    stypenum = get_stype(stype)
    sitemnum = get_object(sitem, "subject", stypenum)
    actionnum = get_action(action)
    otypenum = get_otype(otype)
    oitemnum = get_object(oitem, "object", otypenum)

#    message = "linear(triples:add_triple_sym(#{pstype}, #{psitem}, #{paction}, #{potype}, #{poitem})).\n"
    
#    message1 = "triplestore:add_triple_element(subject(#{stypenum}, #{sitemnum}), #{actionnum}, object(#{otypenum}, #{oitemnum})).\n"
#    message2 = "triplestore:add_triple_element(object(#{otypenum}, #{oitemnum}), #{actionnum}, subject(#{stypenum}, #{sitemnum})).\n"

    seg1 = oitemnum % @nsegs
    seg2 = sitemnum % @nsegs
  
    if (stype != nil) && (sitem != nil) && (action != nil) && (otype != nil) && (oitem != nil) then
#      @sockets[seg1].write(message1)
#      @sockets[seg2].write(message2)
      @segments[seg1].apply_subj(stypenum, sitemnum, actionnum, otypenum, oitemnum)
      @segments[seg2].apply_obj(otypenum, oitemnum, actionnum, stypenum, sitemnum)
      @scount = @scount+1
      if @scount % 10000 == 0 then
        puts(@scount)
      end
    else
      @snilcount = @snilcount + 1
    end
  end

  
#  def set_object(objName, objTypeSpec, objID)
#    message = "dyn_objects:set_object(#{prologify(objName)}, #{objTypeSpec}, #{objID}).\n"
#    @sockets[0].write(message)
#  end

  # prologify makes sure that a string is properly quoted and escaped for quotes, etc
  def prologify(string)
    #All we really need to do, is make sure we substitute \' for ' in the string itself
    if string.is_a?(String) then
      tmp = string.gsub("\'", "\\\\'")
      # And then put the whole thing in single quotes so we're sure it's an atom
      result = "\'#{tmp}\'"
    else
      result = string
    end
    return result
  end

#  def sendall(term_str)
#    @sockets.each do | socket |
#              socket.puts term_str
#            end
#    end


  # Close the connection. Does not invalidate type/object/action symbol associations
  def close
    if @open then
#      @sockets.each do | socket |
#        socket.close
#       end
      flush
      @open = false;
      @nsegs = 0
    end
  end

  # emit cached data
  def flush
    @segments.each do |seg|
      seg.flush
    end
  end
end

