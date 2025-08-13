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
