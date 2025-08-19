# spec/generators/migration_generator_spec.rb
require "rails_helper"
require "tmpdir"

RSpec.describe Etlify::Generators::MigrationGenerator, type: :generator do
  # Run a generator instance in a temporary destination.
  def run_generator_in(dir, args = nil, at: nil)
    # Thor requires a positional `name` argument.
    # Use empty string to trigger your DEFAULT_MIGRATION_FILENAME.
    argv = args.nil? ? [""] : args
    gen = described_class.new(argv, {}, destination_root: dir)
    if at
      Timecop.freeze(at) { gen.invoke_all }
    else
      gen.invoke_all
    end
  end

  def first_file(glob)
    Dir.glob(glob).first
  end

  describe "template presence" do
    it "exposes a migration template" do
      path = File.expand_path(
        "../../lib/generators/etlify/migration/templates/" \
        "create_crm_synchronisations.rb.tt",
        __dir__
      )
      expect(File).to exist(path)
    end
  end

  describe "running the generator" do
    it "creates a migration with the default filename", :aggregate_failures do
      Dir.mktmpdir do |dir|
        run_generator_in(
          dir,
          nil,
          at: Time.utc(2025, 8, 19, 12, 34, 56)
        )

        files = Dir.glob(
          File.join(
            dir,
            "db/migrate/*_create_crm_synchronisations.rb"
          )
        )
        expect(files.size).to eq(1)

        basename = File.basename(files.first)
        expect(basename).to match(/\A20250819123456_create_crm_/)

        content = File.read(files.first)
        expect(content).to include("create_table :crm_synchronisations")
        expect(content).to include("t.string   :crm_id")
        expect(content).to include("t.string   :resource_type, null: false")
        expect(content).to include("t.bigint   :resource_id,   null: false")
        expect(content).to include("t.string   :last_digest")
        expect(content).to include("t.datetime :last_synced_at")
        expect(content).to include("t.string   :last_error")
        expect(content).to include(
          "add_index :crm_synchronisations, :crm_id, unique: true"
        )
        expect(content).to include('name: "idx_crm_sync_on_resource"')
        expect(content).to include(
          "add_index :crm_synchronisations, :last_synced_at"
        )
      end
    end

    it "accepts a custom migration name and camelizes the class", :aggregate_failures do
      Dir.mktmpdir do |dir|
        run_generator_in(
          dir,
          ["init_crm_sync"],
          at: Time.utc(2025, 8, 19, 13, 0, 0)
        )

        files = Dir.glob(File.join(dir, "db/migrate/*_init_crm_sync.rb"))
        expect(files.size).to eq(1)

        content = File.read(files.first)
        expect(content).to match(
          /class InitCrmSync < ActiveRecord::Migration/
        )
        expect(content).to include("create_table :crm_synchronisations")
        expect(content).to include(
          "add_index :crm_synchronisations, :crm_id, unique: true"
        )
      end
    end

    it "targets the current AR major.minor in migration superclass", :aggregate_failures do
      Dir.mktmpdir do |dir|
        run_generator_in(dir, [""])

        path = first_file(
          File.join(
            dir,
            "db/migrate/*_create_crm_synchronisations.rb"
          )
        )
        content = File.read(path)

        expected = "#{ActiveRecord::VERSION::MAJOR}." \
                   "#{ActiveRecord::VERSION::MINOR}"

        expect(content).to match(
          /< ActiveRecord::Migration\[#{Regexp.escape(expected)}\]/
        )
      end
    end

    it "generates unique timestamps on multiple runs", :aggregate_failures do
      Dir.mktmpdir do |dir|
        t1 = Time.utc(2025, 8, 19, 14, 0, 0)
        t2 = t1 + 1

        run_generator_in(dir, [""], at: t1)
        run_generator_in(dir, ["second_one"], at: t2)

        files = Dir.glob(File.join(dir, "db/migrate/*.rb"))
        expect(files.size).to eq(2)

        stamps = files.map { |f| File.basename(f).split("_").first }
        expect(stamps.uniq.size).to eq(2)
      end
    end
  end

  describe ".next_migration_number" do
    it "formats the timestamp as UTC YYYYMMDDHHMMSS" do
      Timecop.freeze(Time.utc(2031, 1, 2, 3, 4, 5)) do
        num = described_class.next_migration_number(nil)
        expect(num).to eq("20310102030405")
      end
    end
  end
end
