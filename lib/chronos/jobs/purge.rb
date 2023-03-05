# frozen-string-literal: true

module Chronos
  module Jobs
    class Purge < Traceable
      def self.signature_keys
        @signature_keys ||= super.concat([:from_schema, :from_target])
      end

      attr_reader :dataset
      attr_reader :trans

      def no_more_purgeable?
        # dataset[:purgeable].size < options[:run_size]
        dataset[:purgeable].empty?
      end

      def caught_up?
        success? and no_more_purgeable?
      end

      def direct_purgation?
        options[:strategy] == :direct
      end

      def archival_purgation?
        options[:strategy] == :archival
      end

      def type
        "purge"
      end

      protected

      def set_default_options
        @default_options ||= super.merge!(
          primary_key: :id,
          primary_key_uuid: false,
          retention_period: "7d"
        )
      end

      def on_init
        super
        init_target
        init_dependents
        init_archive_jobs
        init_purgation_strategy
        init_trace_attr
        init_purge_filter
        init_next_operation
      end

      def init_target
        options[:from_schema], options[:from_target] = options[:from].split(".")
        options[:target_id] = options[:primary_key_uuid] ? :target_uuid : :target_id
      end

      def init_dependents
        options[:dependents] ||= {}
        unless options[:dependents].empty?
          options[:foreign_key] ||= default_foreign_key
          options[:dependents].transform_values! { |v| v || options[:foreign_key] }
        end
        if options[:dependents].any? { |k, v| v.is_a?(Array) }
          raise ArgumentError, "Composite foreign key is NOT supported. Please use surrogate key as foriegn key instead."
        end
      end

      def default_foreign_key
        options[:from_target].classify.foreign_key
      end

      def init_archive_jobs
        options[:archive_jobs] =
          actor(runner).find_jobs(name: "archive", from: options[:from]).keys
      end

      def init_purgation_strategy
        options[:strategy] =
          if !options[:archive_jobs].empty?
            :archival
          elsif !options[:purge_attr].nil?
            :direct
          else
            raise ArgumentError, "Purgation strategy can NOT be determined. " \
              "Direct purge attribute, :purge_attr, MIGHT need to be specified."
          end
      end

      def init_trace_attr
        options[:trace_attr] =
          case options[:strategy]
          when :archival
            :closed_at
          when :direct
            options[:purge_attr]
          end
      end

      def init_purge_filter
        unless options[:filter].nil?
          options[:filter] = eval <<-RUBY, binding, __FILE__, __LINE__ + 1
            ->(dataset, job) { #{options[:filter]} }
          RUBY
        end
      end

      def init_next_operation
        @next_operation = "#{options[:strategy]}_purge".to_sym
      end

      def set_default_values
        super
        init_retention_period
      end

      def init_retention_period
        if options[:retention_period]
          options[:retention_period] =
            parse_time_interval(options[:retention_period])
        end
      end

      def validate
        super
        validate_target
      end

      def validate_target
        if options[:from_schema].nil?
          raise ArgumentError, "Schema for purgation MUST be specified - from: schema.target"
        end
        if options[:from_target].nil?
          raise ArgumentError, "Target for purgation MUST be specified - from: schema.target"
        end
      end

      def reset_states
        super
        reset_dataset
        reset_trans
      end

      def reset_dataset
        @dataset = {purgeable: {}, failed: {}, purged: {}}
      end

      def reset_trans
        @trans = {purgeable: {}, failed: {}, purged: {}}
      end

      def post_init
        options[:primary_key] = options[:primary_key].to_sym
        options[:purge_attr] = options[:purge_attr]&.to_sym
        super
      end

      def update_states
        update_dataset
        update_trans
        super
      end

      %i[dataset trans].each do |state|
        define_method("update_#{state}") do
          send(state).each_pair do |k, v|
            result.each { |r| v.merge!(r.send(state)[k]) }
          end
        end
      end

      def traceable_dataset
        dataset[:purged]
      end

      def no_more_traces?
        dataset[:purgeable].empty?
      end

      def update_summary_for_purge
        if dataset[:purged].size > 0
          @summary += "Purged #{dataset[:purged].size} records " \
            "(#{(dataset[:purged].size / stats[:elapsed_time]).to_i} rps). "
        end

        if dataset[:failed].size > 0
          @summary += "Failed to purge #{dataset[:failed].size} records. "
        end

        @summary += if caught_up?
          "Data purgation caught up: " \
            "synced at - #{trace[:synced_at]}; " \
            "purged_at - #{trace[:traced_at]}. " \
            "Wait for #{options[:wait]} secs before next run. "
        else
          "Continue to purge records."
        end
      end
      alias_method :update_summary_for_direct_purge, :update_summary_for_purge
      alias_method :update_summary_for_archival_purge, :update_summary_for_purge
    end
  end
end
