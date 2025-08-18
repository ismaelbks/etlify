# frozen_string_literal: true

require "rails_helper"
require "etlify/adapters/hubspot_v3_adapter"

RSpec.describe Etlify::Adapters::HubspotV3Adapter do
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
              j["filterGroups"].first["filters"].first["value"] ==
              "john@example.com"
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
        ).and_return({ status: 200, body: { results: [] }.to_json })

        # 2) Create
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy { |b|
            j = JSON.parse(b)
            j["properties"] ==
              { "firstname" => "John", "email" => "john@example.com" }
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

        id = adapter.upsert!(
          object_type: "deals",
          payload: { dealname: "New deal", amount: 1000 }
        )
        expect(id).to eq("9999")
      end
    end

    context "with custom object type" do
      it "searches and creates/updates using the provided custom type" do
        custom_type = "p12345_myobject"

        # 1) Search → no results
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/#{custom_type}/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({ status: 200, body: { results: [] }.to_json })

        # 2) Create
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/#{custom_type}",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy { |b|
            j = JSON.parse(b)
            j["properties"] ==
              { "unique_code" => "ABC-001", "name" => "Custom A" }
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

    it "creates directly when id_property is missing in payload" do
      expect(http).not_to receive(:request).with(:post, /\/search/, anything)

      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/contacts",
        headers: hash_including(
          "Authorization" => "Bearer #{token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json"
        ),
        body: satisfy { |b| JSON.parse(b)["properties"] == { "firstname" => "J" } }
      ).and_return({ status: 201, body: { id: "1001" }.to_json })

      id = adapter.upsert!(
        object_type: "contacts", payload: { firstname: "J" }, id_property: "email"
      )
      expect(id).to eq("1001")
    end

    it "treats malformed 200 search payload as not found then creates" do
      expect(http).to receive(:request).with(
        :post, "https://api.hubapi.com/crm/v3/objects/contacts/search", anything
      ).and_return({ status: 200, body: {}.to_json })

      expect(http).to receive(:request).with(
        :post, "https://api.hubapi.com/crm/v3/objects/contacts", anything
      ).and_return({ status: 201, body: { id: "1002" }.to_json })

      id = adapter.upsert!(
        object_type: "contacts",
        payload: { email: "x@y.com", firstname: "X" },
        id_property: "email"
      )
      expect(id).to eq("1002")
    end

    it "falls back to create when search result has no id" do
      expect(http).to receive(:request).with(
        :post, "https://api.hubapi.com/crm/v3/objects/contacts/search", anything
      ).and_return({ status: 200, body: { results: [{}] }.to_json })

      expect(http).to receive(:request).with(
        :post, "https://api.hubapi.com/crm/v3/objects/contacts", anything
      ).and_return({ status: 201, body: { id: "1003" }.to_json })

      id = adapter.upsert!(
        object_type: "contacts",
        payload: { email: "x@y.com", firstname: "X" },
        id_property: "email"
      )
      expect(id).to eq("1003")
    end

    it "PATCHes and succeeds with 204" do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({ status: 200, body: { results: [{ "id" => "55" }] }.to_json })

      expect(http).to receive(:request).with(
        :patch, "https://api.hubapi.com/crm/v3/objects/contacts/55", anything
      ).and_return({ status: 204, body: "" })

      id = adapter.upsert!(
        object_type: "contacts",
        payload: { email: "john@example.com", firstname: "John" },
        id_property: "email"
      )
      expect(id).to eq("55")
    end

    it "raises ValidationFailed on 422 update" do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({ status: 200, body: { results: [{ "id" => "9" }] }.to_json })

      expect(http).to receive(:request).with(
        :patch, "https://api.hubapi.com/crm/v3/objects/contacts/9", anything
      ).and_return(
        { status: 422, body: { message: "Invalid", category: "X" }.to_json }
      )

      expect {
        adapter.upsert!(
          object_type: "contacts",
          payload: { email: "john@example.com" },
          id_property: "email"
        )
      }.to raise_error(Etlify::ValidationFailed, /Invalid/)
    end

    it "raises RateLimited on 429 create" do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({ status: 200, body: { results: [] }.to_json })

      expect(http).to receive(:request).with(
        :post, "https://api.hubapi.com/crm/v3/objects/contacts", anything
      ).and_return(
        {
          status: 429,
          body: { message: "RL", category: "RATE_LIMITS" }.to_json
        }
      )

      expect {
        adapter.upsert!(
          object_type: "contacts",
          payload: { email: "a@b.com" },
          id_property: "email"
        )
      }.to raise_error(Etlify::RateLimited, /RL/)
    end

    it "raises Unauthorized on 403 search" do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({ status: 403, body: { message: "Forbidden" }.to_json })

      expect {
        adapter.upsert!(
          object_type: "contacts",
          payload: { email: "x@y.com" },
          id_property: "email"
        )
      }.to raise_error(Etlify::Unauthorized, /Forbidden/)
    end

    it "extracts unique value whether key is string or symbol" do
      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/contacts/search",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy { |b|
          j = JSON.parse(b)
          f = j["filterGroups"].first["filters"].first
          f["propertyName"] == "email" && f["value"] == "s@y.com"
        }
      ).and_return({ status: 200, body: { results: [{ "id" => "11" }] }.to_json })

      expect(http).to receive(:request).with(
        :patch, "https://api.hubapi.com/crm/v3/objects/contacts/11",
        headers: hash_including("Accept" => "application/json"),
        body: satisfy { |b|
          j = JSON.parse(b)
          j["properties"] == { "firstname" => "S" }
        }
      ).and_return({ status: 200, body: "{}" })

      id = adapter.upsert!(
        object_type: "contacts",
        payload: { "email" => "s@y.com", firstname: "S" },
        id_property: :email
      )
      expect(id).to eq("11")
    end

    it "raises ApiError with generic message when body is non-JSON" do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({ status: 500, body: "<html>oops</html>" })

      expect {
        adapter.upsert!(
          object_type: "contacts",
          payload: { email: "x@y.com" },
          id_property: "email"
        )
      }.to raise_error(Etlify::ApiError, /HubSpot API request failed/)
    end

    it "wraps transport errors during update into TransportError" do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({ status: 200, body: { results: [{ "id" => "1" }] }.to_json })

      expect(http).to receive(:request).with(
        :patch, /contacts\/1/, anything
      ).and_raise(StandardError.new("tcp reset"))

      expect {
        adapter.upsert!(
          object_type: "contacts",
          payload: { email: "x@y.com", firstname: "X" },
          id_property: "email"
        )
      }.to raise_error(Etlify::TransportError, /tcp reset/)
    end

    it "updates custom object when found" do
      custom_type = "p12345_myobject"

      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/#{custom_type}/search",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: kind_of(String)
      ).and_return({ status: 200, body: { results: [{ "id" => "42" }] }.to_json })

      expect(http).to receive(:request).with(
        :patch,
        "https://api.hubapi.com/crm/v3/objects/#{custom_type}/42",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy { |b|
          j = JSON.parse(b)
          j["properties"] == { "name" => "Custom A" }
        }
      ).and_return({ status: 200, body: "{}" })

      id = adapter.upsert!(
        object_type: custom_type,
        payload: { unique_code: "ABC-001", name: "Custom A" },
        id_property: "unique_code"
      )
      expect(id).to eq("42")
    end

    context "when transport layer fails" do
      it "wraps the error into TransportError", :aggregate_failures do
        expect(http).to receive(:request).and_raise(StandardError.new("boom"))

        expect {
          adapter.upsert!(
            object_type: "contacts",
            payload: { email: "john@example.com", firstname: "John" },
            id_property: "email"
          )
        }.to raise_error(
          Etlify::TransportError, /HTTP transport error: StandardError: boom/
        )
      end
    end

    context "when transport layer raises an Etlify::Error" do
      it "wraps into TransportError and preserves inner class in message" do
        expect(http).to receive(:request).and_raise(
          Etlify::Error.new("boom", status: 500)
        )

        expect {
          adapter.upsert!(
            object_type: "contacts",
            payload: { email: "john@example.com" },
            id_property: "email"
          )
        }.to raise_error(
          Etlify::TransportError, /HTTP transport error: Etlify::Error: boom/
        )
      end
    end

    context "when search returns 401" do
      it "raises Unauthorized" do
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          {
            status: 401,
            body: {
              message: "Invalid credentials",
              category: "INVALID_AUTHENTICATION",
              correlationId: "cid-1"
            }.to_json
          }
        )

        expect {
          adapter.upsert!(
            object_type: "contacts",
            payload: { email: "john@example.com" },
            id_property: "email"
          )
        }.to raise_error(Etlify::Unauthorized, /Invalid credentials.*status=401/)
      end
    end

    context "when search returns 500" do
      it "raises ApiError" do
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          {
            status: 500,
            body: { message: "Server error", category: "INTERNAL_ERROR" }.to_json
          }
        )

        expect {
          adapter.upsert!(
            object_type: "contacts",
            payload: { email: "john@example.com" },
            id_property: "email"
          )
        }.to raise_error(Etlify::ApiError, /Server error.*status=500/)
      end
    end

    context "when search returns 404" do
      it "treats as not found and proceeds to create" do
        # 1) Search -> 404 treated as "not found"
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({ status: 404, body: "" })

        # 2) Create succeeds
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy { |b|
            j = JSON.parse(b)
            j["properties"] == { "email" => "j@e.com", "firstname" => "J" }
          }
        ).and_return({ status: 201, body: { id: "777" }.to_json })

        id = adapter.upsert!(
          object_type: "contacts",
          payload: { email: "j@e.com", firstname: "J" },
          id_property: "email"
        )
        expect(id).to eq("777")
      end
    end

    context "when update returns 429" do
      it "raises RateLimited" do
        # Search finds object
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          { status: 200, body: { results: [{ "id" => "1234" }] }.to_json }
        )

        # Update is rate limited
        expect(http).to receive(:request).with(
          :patch,
          "https://api.hubapi.com/crm/v3/objects/contacts/1234",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          {
            status: 429,
            body: {
              message: "Rate limit exceeded",
              category: "RATE_LIMITS",
              correlationId: "cid-2"
            }.to_json
          }
        )

        expect {
          adapter.upsert!(
            object_type: "contacts",
            payload: { email: "john@example.com", firstname: "John" },
            id_property: "email"
          )
        }.to raise_error(
          Etlify::RateLimited, /Rate limit exceeded.*status=429.*correlationId=cid-2/
        )
      end
    end

    context "when create returns 409 (validation)" do
      it "raises ValidationFailed with details from payload" do
        # Search -> no results
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({ status: 200, body: { results: [] }.to_json })

        # Create -> validation error
        error_payload = {
          message: "Property values were invalid",
          category: "VALIDATION_ERROR",
          correlationId: "cid-3",
          errors: [{ message: "email must be unique", errorType: "CONFLICT" }]
        }

        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({ status: 409, body: error_payload.to_json })

        begin
          adapter.upsert!(
            object_type: "contacts",
            payload: { email: "dup@example.com", firstname: "Dup" },
            id_property: "email"
          )
          raise "expected to raise"
        rescue Etlify::ValidationFailed => e
          expect(e.message).to match(/Property values were invalid/)
          expect(e.status).to eq(409)
          expect(e.category).to eq("VALIDATION_ERROR")
          expect(e.correlation_id).to eq("cid-3")
          expect(e.details).to be_an(Array)
          expect(e.details.first["message"]).to eq("email must be unique")
        end
      end
    end

    context "when update returns 500" do
      it "raises ApiError" do
        # Search finds object
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          { status: 200, body: { results: [{ "id" => "1234" }] }.to_json }
        )

        # Update fails with 500
        expect(http).to receive(:request).with(
          :patch,
          "https://api.hubapi.com/crm/v3/objects/contacts/1234",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({ status: 500, body: { message: "Internal error" }.to_json })

        expect {
          adapter.upsert!(
            object_type: "contacts",
            payload: { email: "john@example.com", firstname: "John" },
            id_property: "email"
          )
        }.to raise_error(Etlify::ApiError, /Internal error.*status=500/)
      end
    end

    it "sends standard JSON headers on create" do
      expect(http).to receive(:request).with(
        :post, /\/crm\/v3\/objects\/deals/,
        headers: include(
          "Authorization" => "Bearer #{token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json"
        ),
        body: kind_of(String)
      ).and_return({ status: 201, body: { id: "d1" }.to_json })

      id = adapter.upsert!(
        object_type: "deals", payload: { dealname: "N", amount: 1_000 }
      )
      expect(id).to eq("d1")
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

    it "raises ApiError on 400 delete" do
      expect(http).to receive(:request).with(
        :delete, "https://api.hubapi.com/crm/v3/objects/contacts/1",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({ status: 400, body: { message: "Bad" }.to_json })

      expect {
        adapter.delete!(object_type: "contacts", crm_id: "1")
      }.to raise_error(Etlify::ApiError, /Bad/)
    end

    it "raises on blank crm_id" do
      expect {
        adapter.delete!(object_type: "contacts", crm_id: "")
      }.to raise_error(ArgumentError)
    end

    it "raises Unauthorized on 401" do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.hubapi.com/crm/v3/objects/contacts/1234",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({ status: 401, body: { message: "No auth" }.to_json })

      expect {
        adapter.delete!(object_type: "contacts", crm_id: "1234")
      }.to raise_error(Etlify::Unauthorized)
    end

    it "raises ApiError on 500" do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.hubapi.com/crm/v3/objects/contacts/1234",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({ status: 500, body: { message: "Server down" }.to_json })

      expect {
        adapter.delete!(object_type: "contacts", crm_id: "1234")
      }.to raise_error(Etlify::ApiError, /Server down/)
    end

    it "wraps transport errors into TransportError" do
      expect(http).to receive(:request).and_raise(StandardError.new("network oops"))

      expect {
        adapter.delete!(object_type: "contacts", crm_id: "1234")
      }.to raise_error(Etlify::TransportError, /network oops/)
    end
  end
end
