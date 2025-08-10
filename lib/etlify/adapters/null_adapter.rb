# frozen_string_literal: true

module Etlify
  module Adapters
    # Adapter no-op pour dev/test
    class NullAdapter
      def upsert!(payload:)
        payload.fetch(:id, SecureRandom.uuid).to_s
      end

      def delete!(crm_id:)
        true
      end
    end
  end
end
