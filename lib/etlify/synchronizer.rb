module Etlify
  class Synchronizer
    attr_accessor(
      :adapter,
      :conf,
      :crm_name,
      :resource
    )
    # main entry point (CRM-aware)
    # @param resource [ActiveRecord::Base]
    # @param crm [Symbol,String]
    def self.call(resource, crm_name:)
      new(resource, crm_name: crm_name).call
    end

    def initialize(resource, crm_name:)
      @resource = resource
      @crm_name = crm_name.to_sym
      @conf    = resource.class.etlify_crms.fetch(@crm_name)
      @adapter = @conf[:adapter].new
    end

    def call
      resource.with_lock do
        if sync_line.stale?(digest)
          crm_id = adapter.upsert!(
            payload: payload,
            id_property: conf[:id_property],
            object_type: conf[:crm_object_type]
          )

          sync_line.update!(
            crm_name: crm_name,
            crm_id: crm_id.presence,
            last_digest: digest,
            last_synced_at: Time.current,
            last_error: nil
          )

          :synced
        else
          sync_line.update!(last_synced_at: Time.current)
          :not_modified
        end
      end
    rescue => e
      sync_line.update!(last_error: e.message)

      :error
    end

    private

    # Compute once to keep idempotency inside the lock
    def digest
      @digest ||= Etlify.config.digest_strategy.call(payload)
    end

    def payload
      @payload ||= resource.build_crm_payload(crm_name: crm_name)
    end

    # Select or build the per-CRM sync line.
    # If you still have has_one, this keeps working but won't handle multi-CRM.
    def sync_line
      resource.crm_synchronisations.find_or_initialize_by(
        crm_name: crm_name
      )
    end
  end
end
