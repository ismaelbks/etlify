module Etlify
  module Serializers
    class BaseSerializer
      # @returns [Hash] serialized and stable payload
      def as_crm_payload(_record)
        raise NotImplementedError
      end
    end
  end
end
