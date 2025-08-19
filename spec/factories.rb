RSpec.shared_context "with companies and users" do
  let!(:company) do
    Company.create!(
      name: "Capsens",
      domain: "capsens.eu"
    )
  end

  let!(:user) do
    User.create!(
      email: "john@capsens.eu",
      full_name: "John Doe",
      company: company
    )
  end
end

def create_sync_for!(record, last_synced_at:)
  CrmSynchronisation.create!(
    resource_type: record.class.name,
    resource_id: record.id,
    last_synced_at: last_synced_at
  )
end
