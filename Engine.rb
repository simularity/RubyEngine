# The Engine class is used to connect to the Simularity High Performance
# Correlation Engine, and can be used to load data into the engine
#
# Author::      Ray Richardson (mailto:ray@simularity.com)
# Copyright::   Copyright 2011 - 2015 Simularity, Inc.
# License::     MIT License

require 'socket'
require 'typhoeus'
require 'json'


class Engine
  def self.hashString(string) 
    h = 5381
    
    string.each_byte do |c|
      # Mod into a positive 64 bit value
      h = (((h << 5) + h) + c) % 0x7fffffffffffffff
    end

    return h
  end

  def self.hash_item(v)
    hashString(v.to_s)
  end

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

    # delete a subject/object triple (action not specified)
    def delete_subj(st, si, ot, oi)
      if @data != "[" then
        @data << ","
      end
      @data << "triple(subject(#{st},#{si}),object(#{ot},#{oi}))"
      @count = @count + 1
      if @count >= @range then
        flush
      end
    end

    # delete an object/subject triple
    def delete_obj(ot, oi, st, si)
      if @data != "[" then
        @data << ","
      end
      @data << "triple(object(#{ot},#{oi}), subject(#{st},#{si}))"
      @count = @count + 1
      if @count >= @range then
        flush
      end
    end
    def set_host(host)
      @host = host
    end
    # send the data to the segment
    def flush
      @data << "]"
      @count = 0
      done = nil
      while !done
        url_str = "http://#{@host}:#{@port}/load_data"
        response = Typhoeus::post(url_str, body: @data)
        json = JSON.parse(response.body)
        if json["status"] == 0 then
          done = true
        else
          sleep 0.5
          puts "Error Flushing Data to Segment"
        end
      end
      @data = "["
    end
  end


  class IterationPrinter
    def initialize(cycle)
      @cycle = cycle
      @count = 0
    end

    def operate(st, si, a, ot, oi)
      @count = @count + 1
      if @count % @cycle == 0 then
        puts @count
      end
    end
  end
  
  public
# These parameters are for the "server" instance of the engine (typically Segment 0)
  def initialize(host="localhost", port=3000, range=10000)
    @open = false
    @range = range
    open(host, port)
    @obj_names = {}
    @subjects = {}
    @objects = {}
    @actions = {}
    @operator = nil

  end

  def set_operator(op)
    @operator = op if op.respond_to?(:operate) || (op == nil)
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
    response = Typhoeus::get(url_str)
        segjson = JSON.parse(response.body)
    @nsegs = 0
    if segjson["status"] == 0 then
      @segments = Array.new
      @sockets = Array.new
      segments = segjson["segments"]
      segments.each do | seg |
        @segments << Segment.new(seg["host"], seg["sport"], @range)
        @nsegs = @nsegs+1
      end
    else
      raise "Could Not retrieve Segments from server"
    end
    # Get the ItemSize
    begin
      url_str = "http://#{host}:#{port}/itemsize"
      response = Typhoeus::get(url_str)
      szjson = JSON.parse(response.body)
      
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

  def set_segment_host(segment, host)
    @segments[segment].set_host(host)
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

    url_str = "http://#{@host}:#{@port}/convert_type?name=#{symbol}&class=subject"
    response = Typhoeus::get(url_str)
    json = JSON.parse(response.body)


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
    url_str = "http://#{@host}:#{@port}/convert_type?name=#{symbol}&class=object"
    response = Typhoeus::get(url_str)
        json = JSON.parse(response.body)

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
    response = Typhoeus::get(url)
        json = JSON.parse(response.body)

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
#        esc = URI.escape("http://#{@host}:#{@port}/convert_object?class=#{typeclass}&name=#{name}&type=#{type}")
        url_str = "http://#{@host}:#{@port}/convert_object?class=#{typeclass}&name=#{name}&type=#{type}"
        response = Typhoeus::get(url_str)
        json = JSON.parse(response.body)

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

    stypenum = get_stype(stype)
    sitemnum = get_object(sitem, "subject", stypenum)
    actionnum = get_action(action)
    otypenum = get_otype(otype)
    oitemnum = get_object(oitem, "object", otypenum)

    # Hash and modulo against a prime. This uses Engine.hash_item
    # So it can be constant across loaders, for now, we use the built in ruby
    # one (which makes all integers odd - hence the prime constant)
    seg1 = Engine.hash_item(oitemnum) % @nsegs
    seg2 = Engine.hash_item(sitemnum) % @nsegs
  
    if (stype != nil) && (sitem != nil) && (action != nil) && (otype != nil) && (oitem != nil) then
      @segments[seg1].apply_subj(stypenum, sitemnum, actionnum, otypenum, oitemnum)
      @segments[seg2].apply_obj(otypenum, oitemnum, actionnum, stypenum, sitemnum)
      @scount = @scount+1
      @operator.operate(stypenum, sitemnum, actionnum, otypenum, oitemnum) if @operator
    else
      @snilcount = @snilcount + 1
    end
  end

  
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

  # Close the connection. Does not invalidate type/object/action symbol associations
  def close
    if @open then
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

  # build a subject from a context and a stamp, within a fixed number of bits
  # We assume the context is an object type
  def build_subject(otype, context, stamp, stampbits)
    ot = get_otype(otype)
    ctx = get_object(context, 'object', ot)
    mask = 2 ** stampbits - 1
    if (stamp & mask) != stamp then
      raise "Stamp #{stamp} too big for mask #{mask}"
    end
    item = (ctx << stampbits) | stamp
    if !item_check(item) then
      raise "Subject #{context} and #{stamp} too big for itemsize #{@max_item}"
    end
    item
  end

  # delete all the subjects (and their objects) based on an Engine Expression
  # this deletes every subject regardless of type or action characteristics
  def delete_expression(expr_str)
    done = nil
    while !done
      
      url_str = "http://#{@host}:#{@port}/expr_receivers?stype=all&action=all"
      response = Typhoeus::post(url_str, body: expr_str)
      json = JSON.parse(response.body)
      if json["status"].to_i != 0 then
        raise "HPCE not present"
      end

      subjects = json["receivers"]
      if subjects.length > 0 then
        if subjects[0]["class"] != "subject" then
          raise "Delete Expression does not specify Subjects"
        end

        first = subjects[0]["item"]
        last = subjects[subjects.length - 1]["item"]

        subjects.each do |subj_json|

          delete_subject(subj_json["type"].to_i, subj_json["item"].to_i)
        end
      else
        done = true
      end
    end
  end

  # delete all triples based in the specified subject
  def delete_subject(type, item)
    done = nil
    while !done
      
      url_str = "http://#{@host}:#{@port}/expr_receivers?otype=all&action=all"
      body = "subject(#{type},#{item})"
    
      response = Typhoeus::post(url_str, body: body)
      json = JSON.parse(response.body)
      if json["status"].to_i != 0 then
        raise "HPCE not present"
      end

      objects = json["receivers"]
      if objects.length > 0 then
        objects.each do |obj_json|
          delete_triple(type, item, obj_json["type"].to_i, obj_json["item"].to_i)
        end
        flush
      else
        done = true
      end
    end
  end

  def delete_triple(st, si, ot, oi)
    sseg = si % @nsegs
    oseg = oi % @nsegs

    @segments[oseg].delete_subj(st, si, ot, oi)
    @segments[sseg].delete_obj(ot, oi, st, si)
  end

  # Cause the triplestore to serialize itself across all segment
  def save
    flush
    url_str = "http://#{@host}:#{@port}/save"
    response = Typhoeus::get(url_str)
        json = JSON.parse(response.body)

    if json["status"] == 0 then
      return true
    else
      return false
    end
  end

  # Cause the triplestore to load data from all segments serialization
  def load
    url_str = "http://#{@host}:#{@port}/restore"
    response = Typhoeus::get(url_str)
        json = JSON.parse(response.body)

    if json["status"] == 0 then
      return true
    else
      return false
    end
  end

  # Empty the triplestore
  def empty
    url_str = "http://#{@host}:#{@port}/empty"
    response = Typhoeus::get(url_str)
        json = JSON.parse(response.body)

    if json["status"] == 0 then
      return true
    else
      return false
    end
  end

end

class DummyEngine

  def initialize(param_hash)
    @params = param_hash
  end
  
  def apply(st, si, a, ot, oi)
    if @params[st] || @params[ot] then
      puts "triple(#{st},#{si},#{a},#{ot},#{oi})"
    end
  end
  
  def build_subject(type, ctx, stamp, bits)
    0
  end
end

class NoApplyEngine < Engine

  def apply(st, si, a, ot, oi)
    nil
  end
end
