require "simplecov"
SimpleCov.start

require "spec_helper"
require "active_record"
require "active_job"
require "rspec"
require "timecop"

require_relative "../lib/etlify"

class ApplicationRecord < ActiveRecord::Base
  self.abstract_class = true
end

require_relative "../app/models/crm_synchronisation"
require_relative "../app/jobs/etlify/sync_job"
require_relative "../lib/etlify/serializers/user_serializer"
require_relative "../lib/etlify/serializers/company_serializer"

require_relative "./factories"

# DB en mémoire
ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")
ActiveRecord::Schema.verbose = false

# Schéma minimal pour tests
ActiveRecord::Schema.define do
  create_table :companies, force: true do |t|
    t.string :name
    t.string :domain
    t.references :user, foreign_key: true
    t.timestamps
  end

  create_table :users, force: true do |t|
    t.string :email
    t.string :full_name
    t.references :company
    t.timestamps
  end

  create_table :crm_synchronisations, force: true do |t|
    t.string  :crm_id
    t.string  :resource_type, null: false
    t.bigint  :resource_id,   null: false
    t.string  :last_digest
    t.datetime :last_synced_at
    t.string :last_error
    t.timestamps
  end

  create_table :teams, force: true do |t|
    t.string :name
    t.timestamps
  end

  create_table :memberships, force: true do |t|
    t.references :user, null: false
    t.references :team, null: false
    t.timestamps
  end

  create_table :tasks, force: true do |t|
    t.string :title
    t.string :owner_type
    t.bigint :owner_id
    t.timestamps
  end

  add_index :crm_synchronisations, :crm_id, unique: true
  add_index :crm_synchronisations, %i[resource_type resource_id], unique: true
  add_index :crm_synchronisations, :last_digest
  add_index :crm_synchronisations, :last_synced_at
end

# Dummy models
class Company < ActiveRecord::Base
  has_many :users, dependent: :nullify
  include Etlify::Model
  etlified_with(
    serializer: Etlify::Serializers::CompanySerializer,
    crm_object_type: "companies",
    id_property: :id,
    dependencies: [:users]
  )
end

class User < ActiveRecord::Base
  include Etlify::Model
  belongs_to :company, optional: true
  has_many :memberships, dependent: :destroy
  has_many :teams, through: :memberships

  etlified_with(
    serializer: Etlify::Serializers::UserSerializer,
    crm_object_type: "contacts",
    id_property: :id,
    dependencies: [:company, :teams]
  )
end

class Team < ActiveRecord::Base
  has_many :memberships, dependent: :destroy
  has_many :users, through: :memberships
end

class Membership < ActiveRecord::Base
  belongs_to :user
  belongs_to :team
end

class Task < ActiveRecord::Base
  belongs_to :owner, polymorphic: true, optional: true

  include Etlify::Model
  etlified_with(
    serializer: Etlify::Serializers::BaseSerializer, # not used by specs
    crm_object_type: "tasks",
    id_property: :id,
    dependencies: [:owner] # <-- triggers polymorphic branch
  )
end

RSpec.configure do |config|
  ActiveJob::Base.logger = Logger.new(nil)
  config.before(type: :generator) do
    allow_any_instance_of(Thor::Shell::Basic).to(
      receive(:say_status)
    )
    allow_any_instance_of(Thor::Shell::Basic).to(
      receive(:say)
    )
  end
  config.before(:each) do
    CrmSynchronisation.delete_all
    Membership.delete_all
    Team.delete_all
    Task.delete_all
    User.delete_all
    Company.delete_all
  end
end
