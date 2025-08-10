module Etlify
  module Serializers
    class UserSerializer < BaseSerializer
      def as_crm_payload(user)
        {
          id: user.id,
          email: user.email,
          full_name: user.full_name,
          company_id: user.company_id
        }
      end
    end
  end
end
