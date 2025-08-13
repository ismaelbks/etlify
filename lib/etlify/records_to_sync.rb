# lib/etlify/records_to_sync.rb
# frozen_string_literal: true

module Etlify
  class RecordsToSync
    class << self
      # Returns: [ { model: ModelClass, records: ActiveRecord::Relation }, ... ]
      #
      # New rule:
      # Include records when the MOST RECENT timestamp among the record and its listed dependencies
      # is within [from_time, now]. Equivalently:
      #   (max(timestamps) >= from_time) AND (max(timestamps) <= now)
      #
      # Implementation (adapter-agnostic):
      # - Build conditions without GREATEST:
      #   * Lower bound (>= from_time): OR across all timestamps (record + deps).
      #   * Upper bound (<= now): AND across all timestamps.
      # - For collection deps (has_many/HABTM), use GROUP BY + HAVING on MAX(dep.updated_at).
      # - For singular deps (belongs_to/has_one), compare directly (or with MAX(...) inside HAVING when grouped).
      def updated_since(from_time)
        raise ArgumentError, "time must respond to #to_time" unless from_time.respond_to?(:to_time)
        from_time = from_time.to_time
        to_time   = Time.now

        eager_load_app_models!

        models_with_etlified_with.map do |model|
          next if abstract_or_missing_table?(model)

          if model.column_names.include?("updated_at")
            deps = extract_dependencies(model)
            { model: model, records: scope_with_dependencies_between(model, deps, from_time, to_time) }
          else
            { model: model, records: model.none }
          end
        end.compact
      end

      # All AR models that define the class method `etlified_with`
      def models_with_etlified_with
        ActiveRecord::Base.descendants.select do |m|
          !abstract_or_missing_table?(m) && m.respond_to?(:etlified_with)
        end
      end

      private

      # Build a single SQL scope per model considering dependencies, selecting rows where
      # max(timestamp set) ∈ [from_time, to_time].
      #
      # Strategy:
      # - LEFT OUTER JOIN each dependency.
      # - If any collection dep exists: GROUP BY PK and put aggregate conditions in HAVING.
      # - Lower bound: (any timestamp >= from_time)  -> OR across all (use MAX(...) for grouped case).
      # - Upper bound: (all timestamps <= to_time)   -> AND across all (use MAX(...) for grouped case).
      # - NULL-safe via COALESCE(ts, '1970-01-01 00:00:00').
      def scope_with_dependencies_between(model, dependencies, from_time, to_time)
        connection = model.connection
        pk         = model.primary_key
        qtbl       = model.quoted_table_name
        qpk        = connection.quote_column_name(pk)

        reflections = dependencies.filter_map { |name| model.reflect_on_association(name) }

        # LEFT JOINs (no N+1)
        join_names = reflections.map(&:name)
        scope = model.left_outer_joins(join_names)

        # Collect raw timestamp columns
        singular_cols   = ["#{qtbl}.updated_at"] # main record
        collection_cols = []

        reflections.each do |ref|
          dep_tbl = ref.klass.quoted_table_name
          dep_col = "#{dep_tbl}.updated_at"
          if ref.collection?
            collection_cols << dep_col
          else
            singular_cols << dep_col
          end
        end

        epoch = "1970-01-01 00:00:00"

        if collection_cols.any?
          # GROUPED CASE: use MAX(...) for all (singular + collection) so everything can live in HAVING.
          scope = scope.group("#{qtbl}.#{qpk}")

          agg_exprs = []
          singular_cols.each   { |c| agg_exprs << "COALESCE(MAX(#{c}), '#{epoch}')" }
          collection_cols.each { |c| agg_exprs << "COALESCE(MAX(#{c}), '#{epoch}')" }

          upper_having = agg_exprs.map { |e| "#{e} <= :to_time" }.join(" AND ")
          lower_having = "(" + agg_exprs.map { |e| "#{e} >= :from_time" }.join(" OR ") + ")"

          having_sql = "#{upper_having} AND #{lower_having}"
          scope.having(model.send(:sanitize_sql_for_conditions, [having_sql, { from_time: from_time, to_time: to_time }]))
        else
          # NON-GROUPED CASE: only singular timestamps -> plain WHERE.
          nonagg_exprs = singular_cols.map { |c| "COALESCE(#{c}, '#{epoch}')" }

          upper_where = nonagg_exprs.map { |e| "#{e} <= :to_time" }.join(" AND ")
          lower_where = "(" + nonagg_exprs.map { |e| "#{e} >= :from_time" }.join(" OR ") + ")"

          where_sql = "#{upper_where} AND #{lower_where}"
          scope.where(model.send(:sanitize_sql_for_conditions, [where_sql, { from_time: from_time, to_time: to_time }]))
        end
      end

      # Extract dependencies from the class-level config.
      # Accepts :dependencies (preferred) and also :dependancies for backward-compat.
      def extract_dependencies(model)
        opts =
          if model.respond_to?(:etlified_with_options)
            model.etlified_with_options
          else
            # If `etlified_with` returns a Hash, use it; otherwise fallback to empty.
            begin
              val = model.etlified_with
              val.is_a?(Hash) ? val : {}
            rescue ArgumentError, NoMethodError
              {}
            end
          end

        raw = opts.with_indifferent_access[:dependencies]
        Array(raw).map { |x| x.to_sym }
      end

      # Eager-load app so that ActiveRecord::Base.descendants is complete.
      # IMPORTANT: do NOT eager-load in test env to avoid wiping out RSpec stubs.
      def eager_load_app_models!
        return unless defined?(Rails) && Rails.respond_to?(:application)
        return if defined?(Rails) && Rails.env.test?

        Rails.application.eager_load!
      end

      # Avoid abstract classes and models without tables (in dev/test)
      def abstract_or_missing_table?(model)
        model.abstract_class? || !model.table_exists?
      rescue ActiveRecord::NoDatabaseError, ActiveRecord::StatementInvalid
        false
      end
    end
  end
end
