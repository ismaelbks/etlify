# frozen_string_literal: true

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

  add_index :crm_synchronisations, :crm_id, unique: true
  add_index :crm_synchronisations, %i[resource_type resource_id], unique: true
  add_index :crm_synchronisations, :last_digest
  add_index :crm_synchronisations, :last_synced_at
end

# Dummy models
class Company < ActiveRecord::Base
  include Etlify::Model
  etlified_with(
    serializer: Etlify::Serializers::CompanySerializer,
    crm_object_type: "companies"
  )
end

class User < ActiveRecord::Base
  include Etlify::Model
  belongs_to :company, optional: true
  etlified_with(
    serializer: Etlify::Serializers::UserSerializer,
    crm_object_type: "contacts"
  )
end
