module Etlify
  module Serializers
    class BaseSerializer
      attr_reader :record

      def initialize(record)
        @record = record
      end
      # @returns [Hash] serialized and stable payload
      def as_crm_payload
        raise NotImplementedError
      end
    end
  end
end
