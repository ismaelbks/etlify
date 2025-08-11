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

  it "records the error on the sync_line when the adapter fails", :aggregate_failures do
    # Adapter raises an API-level error
    adapter = instance_double("Adapter")
    allow(adapter).to receive(:upsert!).and_raise(
      Etlify::ApiError.new("Upsert failed", status: 500)
    )
    allow(Etlify.config).to receive(:crm_adapter).and_return(adapter)

    # It should not raise; it should create the sync row and persist the error
    expect {
      described_class.call(user)
    }.not_to raise_error

    sync = user.reload.crm_synchronisation
    expect(sync).to be_present
    expect(sync.last_error).to eq("Upsert failed")
    expect(sync.crm_id).to be_nil
    expect(sync.last_synced_at).to be_nil
    expect(sync.last_digest).to be_nil
  end

  it "purges any previous error when succeeding", :aggregate_failures do
    adapter = instance_double("Adapter", upsert!: "crm-789")
    allow(Etlify.config).to receive(:crm_adapter).and_return(adapter)

    user.create_crm_synchronisation!
    user.crm_synchronisation.update!(
      last_error: "Previous error",
    )

    expect(user.crm_synchronisation.last_error).to eq("Previous error")
    described_class.call(user)
    expect(user.reload.crm_synchronisation.last_error).to be_nil
  end

  it(
    "does not overwrite previous successful state, only updates last_error on failure",
    :aggregate_failures
  ) do
    # 1) First run succeeds
    ok_adapter = instance_double("Adapter", upsert!: "crm-xyz")
    allow(Etlify.config).to receive(:crm_adapter).and_return(ok_adapter)

    expect { described_class.call(user) }
      .to change { CrmSynchronisation.count }.by(1)

    sync_before = user.reload.crm_synchronisation
    expect(sync_before.crm_id).to eq("crm-xyz")
    expect(sync_before.last_digest).to be_present
    expect(sync_before.last_synced_at).to be_present
    expect(sync_before.last_error).to be_nil

    # 2) Second run fails
    failing_adapter = instance_double("Adapter")
    allow(failing_adapter).to receive(:upsert!).and_raise(
      Etlify::ApiError.new("Network hiccup", status: 429)
    )
    allow(Etlify.config).to receive(:crm_adapter).and_return(failing_adapter)

    user.update!(full_name: "John Doe 2")

    expect {
      described_class.call(user)
    }.not_to raise_error

    sync_after = user.reload.crm_synchronisation
    expect(sync_after.crm_id).to eq("crm-xyz")
    expect(sync_after.last_digest).to eq(sync_before.last_digest)
    expect(sync_after.last_synced_at).to eq(sync_before.last_synced_at)
    expect(sync_after.last_error).to eq("Network hiccup")
  end
end
