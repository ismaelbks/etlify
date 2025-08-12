module Etlify
  module Serializers
    class UserSerializer < BaseSerializer
      def as_crm_payload
        {
          id: record.id,
          email: record.email,
          full_name: record.full_name,
          company_id: record.company_id
        }
      end
    end
  end
end
