# Etlify

**Etlify** is a lightweight Rails DSL and toolkit for idempotent synchronization between your ActiveRecord models and an external CRM.
It provides adapters, serializers, and synchronization jobs so your application can reliably upsert, update, and delete CRM data from your Rails models.

---

## Features

- **Rails-friendly DSL** to declare CRM-synced models.
- **Idempotent synchronization** using digests to avoid redundant API calls.
- **Adapters** for CRM integrations (HubSpot included).
- **Serializer layer** to define exactly how a model maps to CRM fields.
- **Async or inline execution** with ActiveJob.
- **Deleter** for removing records from the CRM when needed.
- **Pluggable architecture** — create your own adapters for other CRMs.

---

## Installation

Add this to your `Gemfile`:

```ruby
gem "etlify"
```

Then run:

```bash
bundle install
bin/rails generate etlify:install
bin/rails generate etlify:migration
rails db:migrate

# Then you can create serializer like this :
bin/rails generate etlify:serializer <MODELNAME>
# example :
bin/rails generate etlify:serializer User
# will create app/serializers/etlify/user_serializer.rb
# or you can create your own as long as you as it respond_to #new(record) and #as_crm_payload
```

---

## Basic Usage

### 1. Configure Etlify

```ruby
# config/initializers/etlify.rb
Etlify.configure do |config|
  config.crm_adapter = MyCrmAdapter.new(
    access_token: ENV["CRM_ACCESS_TOKEN"]
  ) # defaults to Etlify::Adapters::NullAdapter so be sure to change it

  # defaults values
  # config.digest_strategy = Etlify::Digest.method(:stable_sha256)
  # config.logger = Rails.logger
  # config.job_queue_name = "low"
end
```

---

### 2. Declare CRM-synced models

```ruby
class User < ApplicationRecord
  include Etlify::Model

  etlified_with(
    serializer: UserSerializer,
    crm_object_type: "contacts",
    sync_if: ->(user) { user.email.present? }
  )
end
```

---

### 3. Create a serializer

```ruby
class UserSerializer < Etlify::Serializers::BaseSerializer
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

### 4. Trigger synchronization

```ruby
user = User.find(1)

# Async by default
user.crm_sync!
# schedules a Etlify::SyncJob

# Inline execution
user.crm_sync!(async: false)

# Delete from CRM inline (not async)
user.crm_delete!
```

---

## Idempotency

Etlify avoids unnecessary CRM api calls by computing a **digest** of the serialized payload.
If the digest hasn’t changed since the last sync, the operation is skipped (`:not_modified`).

---

## HubSpot Adapter (API v3)

Etlify ships with a `HubspotAdapter` that supports **multiple object types**, both **native** (contacts, companies, deals) and **custom CRM objects**.

### Configuration

```ruby
# config/initializers/etlify.rb
Etlify.configure do |config|
  config.crm_adapter = Etlify::Adapters::HubspotAdapter.new(
    access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"]
  )
  config.digest_strategy = Etlify::Digest.method(:stable_sha256)
  config.logger = Rails.logger
end
```

---

### How it Works

- **`object_type`**: the CRM entity to target (contacts, companies, deals, or a custom object API name).
- **`id_property`** _(optional)_: unique property used for upsert search.
  If provided, the adapter searches for an existing record before creating one.
- If no **`id_property`** is given or no match is found, a new record is created.

---

## Writing Your Own Adapter

An adapter must implement:

```ruby
upsert!(object_type:, payload:, id_property: nil) # returns CRM ID (String)
delete!(object_type:, crm_id:)                    # returns true/false
```

---

## Testing

Run the test suite:

```bash
bundle exec rspec
```

Example stub in tests:

```ruby
fake_adapter = instance_double("Adapter")
allow(Etlify.config).to receive(:crm_adapter).and_return(fake_adapter)
```

---

## License

MIT License. See `LICENSE` for details.
