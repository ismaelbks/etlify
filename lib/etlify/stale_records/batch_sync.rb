module Etlify
  module StaleRecords
    # BatchSync: enqueue or perform sync for all stale records discovered by
    # Finder. It only loads full records when running in synchronous mode to
    # keep memory usage low; in async mode it enqueues jobs by id.
    class BatchSync
      DEFAULT_BATCH_SIZE = 1_000

      # Public: Run a batch sync over all stale records.
      #
      # models:     Optional Array<Class> to restrict scanned models.
      # async:      true => enqueue jobs; false => perform inline.
      # batch_size: # of ids per batch.
      # throttle:   Optional Float (seconds) to sleep between processed records.
      # dry_run:    If true, compute counts but do not enqueue/perform.
      # logger:     IO-like logger; defaults to Etlify.config.logger.
      #
      # Returns a Hash with :total, :per_model, :errors.
      def self.call(models: nil,
                    async: true,
                    batch_size: DEFAULT_BATCH_SIZE,
                    throttle: nil,
                    dry_run: false,
                    logger: Etlify.config.logger)
        new(
          models: models,
          async: async,
          batch_size: batch_size,
          throttle: throttle,
          dry_run: dry_run,
          logger: logger
        ).call
      end

      def initialize(models:, async:, batch_size:, throttle:, dry_run:, logger:)
        @models     = models
        @async      = async
        @batch_size = Integer(batch_size)
        @throttle   = throttle
        @dry_run    = !!dry_run
        @logger     = logger || Etlify.config.logger
      end

      def call
        stats = { total: 0, per_model: {}, errors: 0 }

        Finder.call(models: @models).each do |model, rel|
          processed = process_model(model, rel)
          stats[:per_model][model.name] = processed[:count]
          stats[:total] += processed[:count]
          stats[:errors] += processed[:errors]
        end

        stats
      end

      private

      # Process one model's stale relation (ids-only relation).
      def process_model(model, relation)
        count  = 0
        errors = 0
        pk     = model.primary_key.to_sym

        # Pull ids in batches to avoid loading full records in async mode.
        relation.in_batches(of: @batch_size) do |batch_rel|
          ids = batch_rel.pluck(pk)
          next if ids.empty?

          if @dry_run
            count += ids.size
            next
          end

          if @async
            enqueue_async(model, ids)
            count += ids.size
          else
            # Load full records only when performing inline.
            model.where(pk => ids).find_each(batch_size: @batch_size) do |rec|
              begin
                Etlify::Synchronizer.call(rec)
                count += 1
                sleep(@throttle) if @throttle
              rescue StandardError => e
                log_error(model, rec.id, e)
                errors += 1
              end
            end
          end
        end

        { count: count, errors: errors }
      end

      # Enqueue one job per id without loading the records.
      def enqueue_async(model, ids)
        job_klass = resolve_job_class
        ids.each do |id|
          job_klass.perform_later(model.name, id)
          sleep(@throttle) if @throttle
        end
      rescue StandardError => e
        # If enqueue fails at the batch level, log and re-raise for visibility.
        @logger.error("[Etlify] enqueue failure for #{model.name}: #{e.message}")
        raise
      end

      def resolve_job_class
        klass = Etlify.config.sync_job_class
        klass.is_a?(String) ? klass.constantize : klass
      end

      def log_error(model, id, error)
        @logger.error(
          "[Etlify] sync failure #{model.name}(id=#{id}): #{error.class} " \
          "#{error.message}"
        )
      end
    end
  end
end
