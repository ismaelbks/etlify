require "rails_helper"

RSpec.describe Etlify::Deleter do
  include_context "with companies and users"

  it "deletes on the CRM side if crm_id is present" do
    sync = user.create_crm_synchronisation!(
      crm_id: "crm-42",
      resource_type: "User",
      resource_id: user.id
    )
    adapter = instance_double("Adapter")
    expect(adapter).to receive(:delete!).with(crm_id: "crm-42")
    allow(Etlify.config).to receive(:crm_adapter).and_return(adapter)

    expect(described_class.call(user)).to eq(:deleted)
  end

  it "noop if no crm_id" do
    user.create_crm_synchronisation!(resource_type: "User", resource_id: user.id)
    expect(described_class.call(user)).to eq(:noop)
  end
end
