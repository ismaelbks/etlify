require "rails_helper"

RSpec.describe Etlify::Serializers::UserSerializer do
  describe "#as_crm_payload" do
    subject(:serialized_object) { described_class.new(user).as_crm_payload }

    let(:user) do
      User.new(
        id: 1,
        email: "test@example.com",
        full_name: "Test User",
        company_id: 1
      )
    end

    it "returns the user attributes as a CRM payload" do
      expect(serialized_object).to eq(
        id: 1,
        email: "test@example.com",
        full_name: "Test User",
        company_id: 1
      )
    end

    it "raises an error when method is call from parent class" do
      expect { Etlify::Serializers::BaseSerializer.new(user).as_crm_payload }.to(
        raise_error(NotImplementedError)
      )
    end
  end
end
