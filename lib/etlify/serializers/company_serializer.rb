module Etlify
  module Serializers
    class CompanySerializer < BaseSerializer
      def as_crm_payload
        {
          id: record.id,
          name: record.name,
          domain: record.domain
        }
      end
    end
  end
end
