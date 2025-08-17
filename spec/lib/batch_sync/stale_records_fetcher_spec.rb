# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::BatchSync::StaleRecordsFetcher do
  include_context "with companies and users"

  let(:now)       { Time.utc(2025, 1, 1, 12, 0, 0) }
  let(:from_time) { now - 3.hours }

  before { Timecop.freeze(now) }
  after  { Timecop.return }

  def pair_for(result, model_class)
    result.find { |h| h[:model] == model_class }
  end

  describe ".updated_since" do
    context "without dependencies (default behavior)" do
      it "returns an array of pairs with a relation per etlified model" do
        result = described_class.updated_since(from_time)

        aggregate_failures do
          expect(result).to be_an(Array)
          expect(result.map { |h| h[:model] }).to include(User, Company)

          expect(pair_for(result, User)[:records]).to be_a(ActiveRecord::Relation)
          expect(pair_for(result, Company)[:records]).to be_a(ActiveRecord::Relation)
        end
      end

      it "includes records whose updated_at is within [from_time, now] and excludes older ones" do
        old_user   = user
        mid_user   = User.create!(email: "mid@etlify.test", full_name: "Mid User")
        fresh_user = User.create!(email: "fresh@etlify.test", full_name: "Fresh User")

        # Precise timestamps
        old_user.update_columns(updated_at: from_time - 1.minute)   # too old -> excluded
        mid_user.update_columns(updated_at: from_time)              # exactly at from_time -> included
        fresh_user.update_columns(updated_at: now - 10.minutes)     # recent -> included

        result   = described_class.updated_since(from_time)
        user_ids = pair_for(result, User)[:records].pluck(:id)

        aggregate_failures do
          expect(user_ids).to include(mid_user.id, fresh_user.id)
          expect(user_ids).not_to include(old_user.id)
        end
      end

      it "excludes records strictly newer than now (upper bound safety)" do
        too_new = User.create!(email: "toonew@etlify.test", full_name: "Too New")
        too_new.update_columns(updated_at: now + 60) # in the future -> excluded

        result   = described_class.updated_since(from_time)
        user_ids = pair_for(result, User)[:records].pluck(:id)

        expect(user_ids).not_to include(too_new.id)
      end
    end

    context "with dependencies: [:company]" do
      before do
        allow(User).to(
          receive(:etlified_with_options).and_return({dependencies: [:company]})
        )
      end

      it "includes a user if its company's updated_at is newer than from_time" do
        user.update_columns(updated_at: from_time - 30.minutes)
        company.update_columns(updated_at: from_time + 5.minutes)

        result   = described_class.updated_since(from_time)
        user_rel = pair_for(result, User)[:records]

        expect(user_rel.exists?(id: user.id)).to eq(true)
      end

      it "includes a user if the user itself is fresh even if the company is old" do
        user.update_columns(updated_at: now - 2.minutes)
        company.update_columns(updated_at: from_time - 30.minutes)

        result   = described_class.updated_since(from_time)
        user_rel = pair_for(result, User)[:records]

        expect(user_rel.exists?(id: user.id)).to eq(true)
      end

      it "excludes a user if both user and company are older than from_time" do
        user.update_columns(updated_at: from_time - 20.minutes)
        company.update_columns(updated_at: from_time - 10.minutes)

        result   = described_class.updated_since(from_time)
        user_rel = pair_for(result, User)[:records]

        expect(user_rel.exists?(id: user.id)).to eq(false)
      end

      it "includes a user if either user or company is exactly at from_time" do
        user.update_columns(updated_at: from_time)
        company.update_columns(updated_at: from_time - 1.minute)

        result   = described_class.updated_since(from_time)
        user_rel = pair_for(result, User)[:records]

        expect(user_rel.exists?(id: user.id)).to eq(true)
      end
    end

    context "models without updated_at" do
      before do
        ActiveRecord::Schema.define do
          create_table :foos, force: true do |t|
            t.string :name
          end
        end

        stub_const(
          "Foo",
          Class.new(ApplicationRecord) do
            include Etlify::Model
            self.table_name = "foos"
            etlified_with(
              serializer: Etlify::Serializers::UserSerializer,
              crm_object_type: "foos",
              id_property: :id
            )
          end
        )
      end

      it "returns Model.none for that model", :aggregate_failures do
        result   = described_class.updated_since(from_time)
        foo_pair = pair_for(result, Foo)

        expect(foo_pair).to be_present
        expect(foo_pair[:records]).to be_empty
      end
    end

    context "with has_many dependency (HAVING/MAX path)" do
      before do
        ActiveRecord::Schema.define do
          create_table :projects, force: true do |t|
            t.string :name
            t.references :user, foreign_key: false
            t.timestamps null: true
          end
        end

        stub_const("Project", Class.new(ApplicationRecord) do
          belongs_to :user, optional: true
        end)

        User.has_many :projects, dependent: :destroy
        allow(User).to receive(:etlified_with_options).and_return({ dependencies: [:projects] })
      end

      it "includes the user when any project is updated within the window" do
        user.update_columns(updated_at: from_time - 1.hour) # old user
        p1 = Project.create!(name: "old",  user: user, updated_at: from_time - 2.hours)
        p2 = Project.create!(name: "fresh", user: user, updated_at: now - 5.minutes) # fresh dep

        result   = described_class.updated_since(from_time)
        rel      = pair_for(result, User)[:records]

        expect(rel.exists?(id: user.id)).to eq(true)
      end

      it "excludes the user when all projects and the user are older than from_time" do
        user.update_columns(updated_at: from_time - 2.hours)
        Project.create!(name: "ancient", user: user, updated_at: from_time - 90.minutes)

        result   = described_class.updated_since(from_time)
        rel      = pair_for(result, User)[:records]

        expect(rel.exists?(id: user.id)).to eq(false)
      end
    end
  end
end
