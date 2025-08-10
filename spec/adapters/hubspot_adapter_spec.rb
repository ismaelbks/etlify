# frozen_string_literal: true

require "rails_helper"
require "etlify/adapters/hubspot_adapter"

RSpec.describe Etlify::Adapters::HubspotAdapter do
  let(:token) { "test-token" }
  let(:http)  { instance_double("HttpClient") }

  subject(:adapter) do
    described_class.new(access_token: token, http_client: http)
  end

  describe "#upsert!" do
    context "when object exists (search by id_property) for native type" do
      it "PATCHes the object and returns its id" do
        # 1) Search
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy { |b|
            j = JSON.parse(b)
            j["filterGroups"].first["filters"].first["propertyName"] == "email" &&
              j["filterGroups"].first["filters"].first["value"] == "john@example.com"
          }
        ).and_return(
          { status: 200, body: { results: [{ "id" => "1234" }] }.to_json }
        )

        # 2) Update
        expect(http).to receive(:request).with(
          :patch,
          "https://api.hubapi.com/crm/v3/objects/contacts/1234",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy { |b|
            j = JSON.parse(b)
            j["properties"] == { "firstname" => "John" }
          }
        ).and_return({ status: 200, body: "{}" })

        id = adapter.upsert!(
          object_type: "contacts",
          payload: { email: "john@example.com", firstname: "John" },
          id_property: "email"
        )
        expect(id).to eq("1234")
      end
    end

    context "when object does not exist yet (native type)" do
      it "POSTs a new object and returns its id" do
        # 1) Search → no results
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          { status: 200, body: { results: [] }.to_json }
        )

        # 2) Create
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy { |b|
            j = JSON.parse(b)
            j["properties"] == { "firstname" => "John", "email" => "john@example.com" }
          }
        ).and_return({ status: 201, body: { id: "5678" }.to_json })

        id = adapter.upsert!(
          object_type: "contacts",
          payload: { email: "john@example.com", firstname: "John" },
          id_property: "email"
        )
        expect(id).to eq("5678")
      end
    end

    context "when no id_property is provided (e.g., deals)" do
      it "creates directly and returns the new id" do
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/deals",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy { |b|
            j = JSON.parse(b)
            j["properties"] == { "dealname" => "New deal", "amount" => 1000 }
          }
        ).and_return({ status: 201, body: { id: "9999" }.to_json })

        id = adapter.upsert!(object_type: "deals", payload: { dealname: "New deal", amount: 1000 })
        expect(id).to eq("9999")
      end
    end

    context "with custom object type" do
      it "searches and creates/updates using the provided custom type" do
        # Suppose your custom object type is "p12345_myobject"
        custom_type = "p12345_myobject"

        # 1) Search → no results
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/#{custom_type}/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          { status: 200, body: { results: [] }.to_json }
        )

        # 2) Create
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/#{custom_type}",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy { |b|
            j = JSON.parse(b)
            j["properties"] == { "unique_code" => "ABC-001", "name" => "Custom A" }
          }
        ).and_return({ status: 201, body: { id: "42" }.to_json })

        id = adapter.upsert!(
          object_type: custom_type,
          payload: { unique_code: "ABC-001", name: "Custom A" },
          id_property: "unique_code"
        )
        expect(id).to eq("42")
      end
    end

    it "accepts string or symbol keys in payload" do
      # Search → no results
      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/contacts/search",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: kind_of(String)
      ).and_return({ status: 200, body: { results: [] }.to_json })

      # Create includes both properties
      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/contacts",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy { |b|
          j = JSON.parse(b)
          j["properties"] == { "email" => "a@b.com", "firstname" => "A" }
        }
      ).and_return({ status: 201, body: { id: "314" }.to_json })

      id = adapter.upsert!(
        object_type: "contacts",
        payload: { "email" => "a@b.com", firstname: "A" },
        id_property: "email"
      )
      expect(id).to eq("314")
    end

    it "raises on invalid arguments" do
      expect {
        adapter.upsert!(object_type: "", payload: {})
      }.to raise_error(ArgumentError)

      expect {
        adapter.upsert!(object_type: "contacts", payload: "not a hash")
      }.to raise_error(ArgumentError)
    end
  end

  describe "#delete!" do
    it "returns true on 2xx response" do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.hubapi.com/crm/v3/objects/contacts/1234",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({ status: 204, body: "" })

      expect(adapter.delete!(object_type: "contacts", crm_id: "1234")).to be true
    end

    it "returns false on non-2xx response" do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.hubapi.com/crm/v3/objects/contacts/1234",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({ status: 404, body: "" })

      expect(adapter.delete!(object_type: "contacts", crm_id: "1234")).to be false
    end

    it "raises on invalid arguments" do
      expect {
        adapter.delete!(object_type: "", crm_id: "1")
      }.to raise_error(ArgumentError)

      expect {
        adapter.delete!(object_type: "contacts", crm_id: nil)
      }.to raise_error(ArgumentError)
    end
  end
end
