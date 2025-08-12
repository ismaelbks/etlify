module Etlify
  module Model
    extend ActiveSupport::Concern

    included do
    end

    class_methods do
      # DSL: crm_synced(serializer:, crm_object_type:, sync_if: ->(r){ true })
      def crm_synced(serializer:, crm_object_type:, sync_if: ->(_r) { true })
        class_attribute(
          :etlify_serializer,
          instance_accessor: false,
          default: serializer
        )
        class_attribute(
          :etlify_guard,
          instance_accessor: false,
          default: sync_if
        )
        class_attribute(
          :etlify_crm_object_type,
          instance_accessor: true,
          default: crm_object_type
        )
        has_one(
          :crm_synchronisation,
          as: :resource,
          dependent: :destroy,
          class_name: "CrmSynchronisation"
        )
      end
    end

    # Public API injected
    def crm_synced?
      crm_synchronisation.present?
    end

    def build_crm_payload
      raise_unless_crm_is_configured

      self.class.etlify_serializer.new(self).as_crm_payload
    end

    # @param async [Boolean, nil] prioritaire sur la config globale
    def crm_sync!(async: true)
      return false if self.class.respond_to?(:etlify_guard) && !self.class.etlify_guard.call(self)

      if async
        Etlify::SyncJob.perform_later(self.class.name, id)
      else
        Etlify::Synchronizer.call(self)
      end
    end

    def crm_delete!
      Etlify::Deleter.call(self)
    end

    private

      def raise_unless_crm_is_configured
        return if self.class.respond_to?(:etlify_serializer) && self.class.etlify_serializer

        raise ArgumentError, "crm_synced not configured"
      end
  end
end
