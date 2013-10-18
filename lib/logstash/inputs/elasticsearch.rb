require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/util/socket_peer"

# Read from elasticsearch.
#
# This is useful for replay testing logs, reindexing, etc.
#
# Example:
#
#     input {
#       # Read all documents from elasticsearch matching the given query
#       elasticsearch {
#         host => "localhost"
#         query => "ERROR"
#       }
#     }
#
# * TODO(sissel): Option to keep the index, type, and doc id so we can do reindexing?
class LogStash::Inputs::Elasticsearch < LogStash::Inputs::Base
  config_name "elasticsearch"
  milestone 1

  default :codec, "json"

  # The address of your elasticsearch server
  config :host, :validate => :string, :required => true

  # The http port of your elasticsearch server's REST interface
  config :port, :validate => :number, :default => 9200

  # The index to search
  config :index, :validate => :string, :default => "logstash-*"

  # The query to use
  config :query, :validate => :string, :default => "*"

  # The scroll timeout, corresponding to the 'scroll' parameter in the 'search/scroll' API of elasticsearch
  config :scroll_timeout, :validate=> :string, :default => "5m"

  # The scroll page size, corresponding to the 'size' parameter in the 'search/scroll' API of elasticsearch
  config :scroll_size, :validate=> :number, :default => 1000

  # If true, the event will include meta data of the original elastic document: index ('_index') , document ('_type') and document id ('_id'). By default storted in an 'es_meta' field.
  # This metadata can be used to in reindexing schemes to update rather than append existing indices 
  # ( TODO : make the list of metadata fields configurable (?document version field)  )
  # ( TODO : elasticsearch output will need to use the bulk/create API instead
  #          of bulk/index API to avoid overwriting existing documents in the target index (idempotency) )
  # ( TODO : consider alternative approach: include_meta will take the 'hit' document (including metadata) + output codec for
  #          elastic to index only the '_source' field  )
  config :include_meta, :validate=> :boolean, :default => false
  
  # The (fixed) field name under which metadata of the original elastic document is stored
  # ( TODO : should this be a dynamic sprintf field ?) 
  config :meta_field, :validate=> :string, :default => "es_meta"

  public
  def register
    require "ftw"
    @agent = FTW::Agent.new
    params = {
      "q" => @query,
      "scroll" => @scroll_timeout,
      "size" => @scroll_size,
    }
    @url = "http://#{@host}:#{@port}/#{@index}/_search?#{encode(params)}"
  end # def register

  private
  def encode(hash)
    return hash.collect do |key, value|
      CGI.escape(key.to_s) + "=" + CGI.escape(value.to_s)
    end.join("&")
  end # def encode

  public
  def run(output_queue)
    @logger.debug("scroll initialization",:request => @url)
    response = @agent.get!(@url)
    json = ""
    response.read_body { |c| json << c }
    result = JSON.parse(json)
    scroll_url = @url
    while true
      break if result.nil?
      if result["error"]
        @logger.warn(result["error"], :request => scroll_url)
        break
      end
      
      hits = result["hits"]["hits"]
      break if hits.empty?

      result["hits"]["hits"].each do |hit|
        event = hit["_source"]
        if @include_meta 
           event[@meta_field] = { 
               '_index' => hit['_index'],
               '_type' => hit['_type'],
               '_id' => hit['_id']
           }
        end
        # Hack to make codecs work
        @codec.decode(event.to_json) do |event|
          decorate(event)
          output_queue << event
        end
      end

      scroll_id = result["_scroll_id"] 
      if scroll_id.nil?
         @logger.warn("no _scroll_id in result", :request => scroll_url)
         break 
      end

      # Fetch until we get no hits
      scroll_params = { 
        "scroll" => @scroll_timeout,
        "scroll_id" => scroll_id
      }
      scroll_url = "http://#{@host}:#{@port}/_search/scroll?#{encode(scroll_params)}"
      @logger.debug("scroll request",:request => scroll_url)
      response = @agent.get!(scroll_url)
      json = ""
      response.read_body { |c| json << c }
      result = JSON.parse(json)
    end
  rescue LogStash::ShutdownSignal
    # Do nothing, let us quit.
  end # def run
end # class LogStash::Inputs::Elasticsearch
