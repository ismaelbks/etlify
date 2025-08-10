require "json"
require "digest"

module Etlify
  module Digest
    # Computes a stable SHA256 from a Hash/Array/Scalar,
    # sorting keys and removing volatile values.
    #! @param payload [Hash|Array|Scalar] the input data
    #! @return [String] the computed SHA256 digest
    def self.stable_sha256(payload)
      normalized = normalize(payload)
      ::Digest::SHA256.hexdigest(JSON.generate(normalized))
    end

    def self.normalize(obj)
      case obj
      when Hash
        obj.keys.sort.map { |k| [k, normalize(obj[k])] }.to_h
      when Array
        obj.map { |v| normalize(v) }
      else
        obj
      end
    end
  end
end
