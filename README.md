# Etlify

> Rails-first, idempotent synchronisation between your ActiveRecord models and your CRM. HubSpot is supported out of the box; other CRMs can be plugged in via adapters.

This gem has been designed by [Capsens, a fintech web and mobile agency based in Paris](https://capsens.eu/).

---

## Why Etlify? (Context & Intended Use)

In internal products, it is common to persist domain data in Rails while also mirroring a subset of it into a CRM for marketing, sales or support workflows. Etlify provides a small, dependable toolkit to **declare** which models are CRM-backed, **serialise** them into CRM payloads, and **synchronise** them in an **idempotent** fashion so repeated calls are safe and efficient.

Etlify sits beside your app; it does **not** try to own your domain or background processing. It integrates naturally with ActiveRecord and ActiveJob so you keep your current architecture and simply “switch on” CRM sync where you need it.

---

## Features at a glance

| Area          | What you get                                                  | Why it helps                                        |
| ------------- | ------------------------------------------------------------- | --------------------------------------------------- |
| DSL           | `include Etlify::Model` + `etlified_with(...)` on your models | Opt-in sync with a single line; clear, local intent |
| Serialisers   | A base class to turn a model into a CRM payload               | Keeps mapping logic where it belongs; easy to test  |
| Adapters      | HubSpot adapter included; plug your own                       | Swap CRMs without touching model code               |
| Idempotence   | Stable digest of the last synced payload                      | Avoids redundant API calls; safe to retry           |
| Jobs          | `crm_sync!` enqueues an ActiveJob (`SyncJob`) or runs inline  | Fits your queue; simple to trigger                  |
| Delete        | `crm_delete!` to remove a record from the CRM                 | Keeps both sides consistent                         |
| Configuration | Logger, queue name, digest strategy, adapter                  | Control behaviour without scattering settings       |

---

## Requirements & Compatibility

- **Ruby:** 3.0+
- **Rails:** 6.1+ (ActiveRecord & ActiveJob)
- **Datastore:** A relational database supported by ActiveRecord for storing sync state.
- **Threading/Jobs:** Any ActiveJob backend (Sidekiq, Delayed Job, etc.).

> These ranges reflect typical modern Rails setups. If you run older stacks, test in your environment.

---

## Installation

Add the gem to your application:

```ruby
# Gemfile
gem "etlify"
```

Then install and run the generators:

```bash
bundle install

# Install initializer(s)
bin/rails generate etlify:install

# Install sync-state tables
bin/rails generate etlify:migration CreateCrmSynchronisations
bin/rails db:migrate

# Generate a serializer for a model (optional helper)
bin/rails generate etlify:serializer User
# => creates app/serializers/etlify/user_serializer.rb
```

> You may create your own serializer class manually as long as it responds to `#new(record)` and `#as_crm_payload`.

---

## Configuration

Create `config/initializers/etlify.rb`:

```ruby
# config/initializers/etlify.rb
Etlify.configure do |config|
  # Choose the CRM adapter (default is a NullAdapter; be sure to change it)
  config.crm_adapter = Etlify::Adapters::HubspotAdapter.new(
    access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"]
  )

  # Optional settings (shown with defaults)
  config.digest_strategy = Etlify::Digest.method(:stable_sha256) # -> String
  config.logger          = Rails.logger
  config.job_class       = "Etlify::SyncJob"
  config.job_queue_name  = "low"
end
```

### Declaring a CRM-synced model

```ruby
# app/models/user.rb
class User < ApplicationRecord
  include Etlify::Model

  has_many :investments, dependent: :destroy

  etlified_with(
    serializer: UserSerializer,
    crm_object_type: "contacts",
    # Only sync when an email exists
    sync_if: ->(user) { user.email.present? },
    # useful if your serialization include dependencies
    dependencies: [:investments]
  )
end
```

### Writing a serializer

```ruby
# app/serializers/etlify/user_serializer.rb
class UserSerializer < Etlify::Serializers::BaseSerializer
  # Must return a Hash that matches your CRM field names
  def as_crm_payload(user)
    {
      email: user.email,
      firstname: user.first_name,
      lastname: user.last_name
    }
  end
end
```

---

## Usage

### Synchronise a single record

```ruby
user = User.find(1)

# Async by default (enqueues Etlify.config.sync_job_class (default: "Etlify::SyncJob") on the configured queue)
user.crm_sync!

# Run inline (no job)
user.crm_sync!(async: false)
```

### Delete a record from the CRM

```ruby
# Inline delete (not enqueued)
user.crm_delete!
```

### Custom serializer example

```ruby
# app/serializers/etlify/company_serializer.rb
class CompanySerializer < Etlify::Serializers::BaseSerializer
  # Keep serialisation small and predictable
  def as_crm_payload(company)
    {
      name: company.name,
      domain: company.domain,
      hs_lead_status: company.lead_status # Example custom property
    }
  end
end
```

### Swapping adapters

```ruby
# Switch to a different adapter at runtime (for a test, a rake task, etc.)
Etlify.config.crm_adapter = MyCrmAdapter.new(api_key: ENV["MYCRM_API_KEY"])
```

---

## Batch synchronisation

Beyond single-record syncs, Etlify ships with a batch API to **synchronise all
records that changed since a given point in time**. This is useful to:

- recover from outages or CRM downtime,
- run periodic re-syncs (e.g. from a cron job),
- debug the synchronisation logic against a controlled dataset.

### API

```ruby
# Async (default): enqueue one job per record
Etlify::BatchSync::StaleRecordsSyncer.call(since: 3.hours.ago)

# Synchronous: run inline in the current process
Etlify::BatchSync::StaleRecordsSyncer.call(
  since: 1.day.ago,
  async: false
)

# Custom batch size (number of IDs per SQL batch)
Etlify::BatchSync::StaleRecordsSyncer.call(
  since: 1.week.ago,
  batch_size: 1000
)

# Pass ActiveJob options (queue name, priority, etc.)
Etlify::BatchSync::StaleRecordsSyncer.call(
  since: 2.days.ago,
  job_options: { queue: "etlify", priority: 10 }
)
```

### How it works

- `StaleRecordsFetcher` inspects all models that declared `etlified_with` and
  builds SQL scopes to find records whose **own timestamp or dependencies’
  timestamps** are within `[since, now]`.
- Results are projected down to **only the primary key** to keep queries light.
- `StaleRecordsSyncer` then iterates over those records in **batches**:
  - in _async_ mode (default), enqueues a `SyncJob` for each record,
  - in _sync_ mode, calls the `Synchronizer` directly inline.

> ⚡️ Tip: prefer `async: true` in production so Rails web or rake processes
> aren’t blocked; let your background workers handle the flow.

---

## How idempotence works

- Before sending anything to the CRM, Etlify builds the payload via your serializer and computes a **stable digest** (SHA-256 by default) of that payload.
- Etlify stores the **last successful digest** alongside the CRM ID for that record in your application database.
- On subsequent syncs, if the **new digest equals the last stored digest**, Etlify **skips** the remote call and returns `:not_modified`.
- If the digest **differs**, Etlify upserts the record remotely and updates the stored digest.

You can customise the digest strategy:

```ruby
Etlify.config.digest_strategy = lambda do |payload|
  # Always use deterministic JSON generation for hashing
  Digest::SHA256.hexdigest(JSON.dump(payload))
end
```

> Tip: Keep your serializer output **stable** (e.g. avoid unordered hashes or volatile timestamps) so that digests are meaningful.

---

## HubSpot adapter (API v3)

Etlify ships with `Etlify::Adapters::HubspotAdapter`. It supports native objects (e.g. **contacts**, **companies**, **deals**) and custom objects by API name.

### Configuration

```ruby
Etlify.configure do |config|
  config.crm_adapter = Etlify::Adapters::HubspotAdapter.new(
    access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"]
  )
end
```

### Behaviour

- `object_type`: the target entity, e.g. `"contacts"`, `"companies"`, `"deals"`, or the API name of a custom object.
- `id_property` (optional): if your upsert should search for an existing record by a unique property (e.g. `"email"` for contacts), the adapter uses it to find-or-create.
- If no match is found (or no `id_property` is provided), the adapter **creates** a new record.

### Example: Contact upsert

```ruby
class User < ApplicationRecord
  include Etlify::Model

  etlified_with(
    serializer: UserSerializer,
    crm_object_type: "contacts",
    sync_if: ->(user) { user.email.present? }
  )
end

# Later
user.crm_sync! # Adapter performs an upsert
```

### Example: Custom object

```ruby
class Subscription < ApplicationRecord
  include Etlify::Model

  etlified_with(
    serializer: SubscriptionSerializer,
    crm_object_type: "p1234567_subscription" # Custom object API name
  )
end
```

---

## Writing your own adapter

Implement the following interface:

```ruby
module Etlify
  module Adapters
    class MyCrmAdapter
      # Must return the remote CRM ID as a String
      def upsert!(object_type:, payload:, id_property: nil)
        # Call your CRM API to create or update
        # Return the CRM id (e.g. "12345")
      end

      # Must return true/false
      def delete!(object_type:, crm_id:)
        # Call your CRM API to delete the record
        # Return true when the remote says it has been removed
      end
    end
  end
end
```

> Keep your adapter stateless and pure. Pass all needed options explicitly and let your initializer construct it with credentials.

---

## Best practices · FAQ · Troubleshooting

### General tips

- **Start small**: sync only the fields you truly need in your serializer. You can add more later.
- **Stable payloads**: avoid non-deterministic fields (timestamps, random IDs) in the payload; they defeat idempotence.
- **Guard with `sync_if`**: skip incomplete records (e.g. no email) to reduce noise.
- **Observe logs**: Etlify uses your configured logger; in development, check the console.
- **Queue selection**: route `SyncJob` to a dedicated low-priority queue to keep UX jobs snappy.

### Common questions

- **Nothing seems to happen when I call `crm_sync!`**
  Ensure you ran the migration generator and migrated the database. Also verify your `sync_if` predicate returns `true` and the serializer returns a Hash.

- **My payload keeps re-syncing even when nothing changed**
  Confirm your serializer output is stable and keys are consistently ordered/typed. If you add transient data, the digest will change on every run.

- **How do I force a refresh?**
  Change the payload (or clear the stored digest for that record) and run `crm_sync!` again. You can also add a temporary flag inside your serializer if needed.

- **Where is the CRM ID stored?**
  Etlify maintains sync state (last digest and remote ID) in your app’s database so it can skip or delete correctly.

- **Can I batch synchronise?**
  Use `Etlify::BatchSync::StaleRecordsSyncer.call(...)`. Keep batches small and let your queue handle back-pressure.

### Debugging checklist

- Credentials present and valid (e.g. `HUBSPOT_PRIVATE_APP_TOKEN`).
- Adapter set (default is a no-op NullAdapter).
- Jobs worker running (when using async).
- Serializer returns a Hash with the expected field names.
- Database table for sync state exists and is reachable.

---

## Testing

Run the test suite:

```bash
bundle exec rspec
```

### Stubbing the adapter in specs

```ruby
# In your spec
fake_adapter = instance_double("Adapter")
allow(Etlify.config).to receive(:crm_adapter).and_return(fake_adapter)
allow(fake_adapter).to receive(:upsert!).and_return("crm_123")
allow(fake_adapter).to receive(:delete!).and_return(true)

user = create(:user, email: "someone@example.com")
user.crm_sync!

expect(fake_adapter).to have_received(:upsert!).with(
  object_type: "contacts",
  payload: hash_including(email: "someone@example.com"),
  id_property: anything
)
```

> For end-to-end tests, use VCR or WebMock around your adapter, but prefer unit-level tests against your serialisers and model logic.

---

## Adapters included

- `Etlify::Adapters::NullAdapter` (default; no-op)
- `Etlify::Adapters::HubspotAdapter` (API v3)

---

## Licence

**MIT** — see `LICENSE`.

---

## Maintainers & Support

This library is maintained internally. Please open an issue if you need enhancements or have questions.
