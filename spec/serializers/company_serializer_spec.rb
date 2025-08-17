require "rails_helper"

RSpec.describe Etlify::Serializers::CompanySerializer do
  describe "#as_crm_payload" do
    subject(:serialized_object) { described_class.new(company).as_crm_payload }

    let(:company) do
      Company.new(
        id: 1,
        name: "Test Company",
        domain: "testcompany.com"
      )
    end

    it "returns the company attributes as a CRM payload" do
      expect(serialized_object).to eq(
        id: 1,
        name: "Test Company",
        domain: "testcompany.com"
      )
    end
  end
end
