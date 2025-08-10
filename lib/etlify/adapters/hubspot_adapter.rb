require "json"
require "uri"
require "net/http"

module Etlify
  module Adapters
    # HubSpot Adapter (API v3) with per-call object type.
    # It supports native objects (e.g., "contacts", "companies", "deals") and custom objects (e.g., "p12345_myobject").
    #
    # Usage:
    #   adapter = Etlify::Adapters::HubspotAdapter.new(access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"])
    #
    #   # Upsert a contact by email:
    #   adapter.upsert!(object_type: "contacts", payload: { email: "john@example.com", firstname: "John" }, id_property: "email")
    #
    #   # Create a deal without an id_property:
    #   adapter.upsert!(object_type: "deals", payload: { dealname: "New deal", amount: 1000 })
    #
    #   # Delete:
    #   adapter.delete!(object_type: "contacts", crm_id: "1234567890")
    class HubspotAdapter
      API_BASE = "https://api.hubapi.com"

      # @param access_token [String] HubSpot private app token
      # @param http_client [#request] Optional HTTP client for tests. Signature: request(method, url, headers:, body:)
      def initialize(access_token:, http_client: nil)
        @access_token = access_token
        @http         = http_client || DefaultHttp.new
      end

      # Upsert by searching on id_property (if provided), otherwise create directly.
      # @param object_type [String] HubSpot CRM object type (e.g., "contacts", "companies", "deals", or a custom object)
      # @param payload [Hash] Properties for the object
      # @param id_property [String, nil] Unique property used to search and upsert (e.g., "email" for contacts, "domain" for companies)
      # @return [String, nil] HubSpot hs_object_id as string or nil if not available
      def upsert!(object_type:, payload:, id_property: nil)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)

        properties   = payload.dup
        unique_value = nil

        if id_property
          # Extract unique value whether key is provided as String or Symbol
          unique_value = properties.delete(id_property) || properties.delete(id_property.to_sym)
        end

        object_id =
          if id_property && unique_value
            find_object_id_by_property(object_type, id_property, unique_value)
          end

        if object_id
          update_object(object_type, object_id, properties)
          object_id.to_s
        else
          create_object(object_type, properties, id_property, unique_value)
        end
      end

      # Delete an object by hs_object_id.
      # @param object_type [String]
      # @param crm_id [String]
      # @return [Boolean] true on 2xx response
      def delete!(object_type:, crm_id:)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "crm_id must be provided" if crm_id.nil? || crm_id.to_s.empty?

        path = "/crm/v3/objects/#{object_type}/#{crm_id}"
        resp = request(:delete, path)
        resp[:status].between?(200, 299)
      end

      private

      # Simple Net::HTTP client used by default (dependency-free)
      class DefaultHttp
        def request(method, url, headers: {}, body: nil)
          uri  = URI(url)
          http = Net::HTTP.new(uri.host, uri.port)
          http.use_ssl = uri.scheme == "https"

          klass = {
            get:    Net::HTTP::Get,
            post:   Net::HTTP::Post,
            patch:  Net::HTTP::Patch,
            delete: Net::HTTP::Delete
          }.fetch(method) { raise ArgumentError, "Unsupported method: #{method.inspect}" }

          req = klass.new(uri.request_uri, headers)
          req.body = body if body

          res = http.request(req)
          { status: res.code.to_i, body: res.body }
        end
      end

      def request(method, path, body: nil, query: {})
        url = API_BASE + path
        url += "?#{URI.encode_www_form(query)}" unless query.empty?

        headers = {
          "Authorization" => "Bearer #{@access_token}",
          "Content-Type"  => "application/json",
          "Accept"        => "application/json"
        }

        raw_body = body && JSON.dump(body)
        @http.request(method, url, headers: headers, body: raw_body).tap do |res|
          res[:json] = parse_json_safe(res[:body])
        end
      end

      def parse_json_safe(str)
        return nil if str.nil? || str.empty?
        JSON.parse(str)
      rescue JSON::ParserError
        nil
      end

      def find_object_id_by_property(object_type, property, value)
        path = "/crm/v3/objects/#{object_type}/search"
        body = {
          filterGroups: [
            { filters: [{ propertyName: property.to_s, operator: "EQ", value: value }] }
          ],
          properties: ["hs_object_id"],
          limit: 1
        }
        resp = request(:post, path, body: body)
        if resp[:status] == 200 && resp[:json].is_a?(Hash)
          results = resp[:json]["results"]
          return results.first["id"] if results.is_a?(Array) && results.any?
        end
        nil
      end

      def update_object(object_type, object_id, properties)
        path = "/crm/v3/objects/#{object_type}/#{object_id}"
        body = { properties: stringify_keys(properties) }
        request(:patch, path, body: body)
      end

      def create_object(object_type, properties, id_property, unique_value)
        path  = "/crm/v3/objects/#{object_type}"
        props = stringify_keys(properties)

        # If a unique property was provided and its value was extracted, ensure it is present on creation
        if id_property && unique_value && !props.key?(id_property.to_s)
          props[id_property.to_s] = unique_value
        end

        resp = request(:post, path, body: { properties: props })
        if resp[:status].between?(200, 299) && resp[:json].is_a?(Hash) && resp[:json]["id"]
          resp[:json]["id"].to_s
        end
      end

      def stringify_keys(hash)
        hash.each_with_object({}) { |(k, v), h| h[k.to_s] = v }
      end
    end
  end
end
