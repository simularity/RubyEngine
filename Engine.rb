require 'socket'
require 'net/http/persistent'
require 'json'

class Engine
  
# These parameters are for the "server" instance of the engine
  def initialize(host="localhost", port=3000)
    @open = false
    open(host, port)
    @obj_names = {}
    @subjects = {}
    @objects = {}
    @actions = {}
  end

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
        @segments << [seg["segment"], seg["host"], seg["port"]]
        @sockets << TCPSocket.open(seg["host"], seg["port"])
        @nsegs = @nsegs+1
      end
    else
      raise "Could Not retrieve Segments from server"
    end

    @scount = 0
    @snilcount = 0
  end

  def item_check(item)
    if item.is_a?(Integer) then
      if item > 0
        if item <= 0xffffffff
          return true
        end
      end
    end
    return false
  end

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

  def action_check(action)
    if type.is_a?(Integer) then
      if type > 0 then
        if type <= 0x1fff then
          return true;
        end
      end
    end
    return false
  end

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

  def no_spaces(name)
    if name.is_a?(String) then
      name.gsub(' ', '_')
    else
      name
    end
  end
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
    
    message1 = "triplestore:add_triple_element(subject(#{stypenum}, #{sitemnum}), #{actionnum}, object(#{otypenum}, #{oitemnum})).\n"
    message2 = "triplestore:add_triple_element(object(#{otypenum}, #{oitemnum}), #{actionnum}, subject(#{stypenum}, #{sitemnum})).\n"

    seg1 = oitemnum % @nsegs
    seg2 = sitemnum % @nsegs
  
    if (stype != nil) && (sitem != nil) && (action != nil) && (otype != nil) && (oitem != nil) then
      @sockets[seg1].write(message1)
      @sockets[seg2].write(message2)
      @scount = @scount+1
      if @scount % 10000 == 0 then
        puts(@scount)
      end
    else
      @snilcount = @snilcount + 1
    end
  end

  def set_object(objName, objTypeSpec, objID)
    message = "dyn_objects:set_object(#{prologify(objName)}, #{objTypeSpec}, #{objID}).\n"
    @sockets[0].write(message)
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

  def sendall(term_str)
    @sockets.each do | socket |
              socket.puts term_str
            end
    end

  def close
    if @open then
      @sockets.each do | socket |
        socket.close
      end
      @open = false;
      @nsegs = 0
    end
  end
end
