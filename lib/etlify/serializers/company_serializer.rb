module Etlify
  module Serializers
    class CompanySerializer < BaseSerializer
      def as_crm_payload(company)
        {
          id: company.id,
          name: company.name,
          domain: company.domain
        }
      end
    end
  end
end
