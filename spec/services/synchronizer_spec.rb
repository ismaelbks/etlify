require "rails_helper"

RSpec.describe Etlify::Synchronizer do
  include_context "with companies and users"

  it "creates the row and updates the digest", :aggregate_failures do
    adapter = instance_double("Adapter", upsert!: "crm-123")
    allow(Etlify.config).to receive(:crm_adapter).and_return(adapter)

    expect { described_class.call(user) }
      .to change { CrmSynchronisation.count }.by(1)

    sync = user.crm_synchronisation
    expect(sync.crm_id).to eq("crm-123")
    expect(sync.last_digest).to be_present
    expect(sync.last_synced_at).to be_present
  end

  it "is idempotent if the digest hasn't changed", :aggregate_failures do
    adapter = instance_double("Adapter")
    allow(adapter).to receive(:upsert!).and_return("crm-456")
    allow(Etlify.config).to receive(:crm_adapter).and_return(adapter)

    first = described_class.call(user)
    second = described_class.call(user)

    expect(first).to eq(:synced)
    expect(second).to eq(:not_modified)
  end
end
