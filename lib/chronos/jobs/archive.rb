# frozen-string-literal: true

module Chronos
  module Jobs
    class Archive < Traceable
      def self.signature_keys
        @signature_keys ||=
          super.concat([:from_schema, :from_target, :to_schema, :to_target])
      end

      attr_reader :dataset
      attr_reader :trans
      attr_reader :trans_logs

      def default_operation
        :resolve_pending
      end

      def archiving?
        @operation == :archive_new
      end

      def no_more_new?
        # dataset[:new].size < options[:run_size]
        dataset[:new].empty?
      end

      def caught_up?
        success? and archiving? and no_more_new?
      end

      def resolving_pending?
        @operation == :resolve_pending
      end

      def no_more_pending?
        # dataset[:pending].size < options[:run_size]
        dataset[:pending].empty?
      end

      def pending_resolved?
        success? and resolving_pending? and no_more_pending?
      end

      def traceable?
        archiving? and super
      end

      def type
        "archive"
      end

      protected

      def set_default_options
        @default_options ||= super.merge!(
          primary_key: :id,
          primary_key_uuid: false,
          trace_drift: 60
        )
      end

      def on_init
        super
        init_target
        init_filter_attrs
        init_archive_filter
        init_transform_processor
      end

      def init_target
        options[:from_schema], options[:from_target] = options[:from].split(".")
        options[:to_schema], options[:to_target] = options[:to].split(".")
        options[:to_target] ||= options[:from_target]
        options[:target_id] = 
          options[:primary_key_uuid] ? :target_uuid : :target_id
        options[:chronos_archive_transactions] = 
          options[:primary_key_uuid] ? 
            :chronos_uuid_archive_transactions : 
            :chronos_archive_transactions
        options[:chronos_archive_transaction_logs] = 
          options[:primary_key_uuid] ? 
            :chronos_uuid_archive_transaction_logs : 
            :chronos_archive_transaction_logs
      end

      def init_filter_attrs
        options[:reject_attrs] ||= []
        options[:reject_attrs].map!(&:to_sym)
        options[:select_attrs] ||= []
        options[:select_attrs].map!(&:to_sym)
        options[:rename_attrs] ||= {}
        options[:rename_attrs].transform_keys!(&:to_sym)
      end

      def init_archive_filter
        unless options[:filter].nil?
          # options[:filter] = eval("->(dataset, job) { #{options[:filter]} }")
          options[:filter] = eval <<-RUBY, binding, __FILE__, __LINE__ + 1
            ->(dataset, job) { #{options[:filter]} }
          RUBY
        end
      end

      def init_transform_processor
        unless options[:transform].nil?
          options[:transform] = eval <<-RUBY, binding, __FILE__, __LINE__ + 1
            ->(row, job) { #{options[:transform]}; row }
          RUBY
        end
      end

      def validate
        super
        validate_target
      end

      def validate_target
        if options[:from_schema].nil?
          raise ArgumentError, "Schema to archive from MUST be specified - from: schema.target"
        end
        if options[:from_target].nil?
          raise ArgumentError, "Target to archive from MUST be specified - from: schema.target"
        end
        if options[:to_schema].nil?
          raise ArgumentError, "Schema to archive to MUST be specified - to: schema.target"
        end
        if options[:to_target].nil?
          raise ArgumentError, "Target to archive to MUST be specified - to: schema.target"
        end
      end

      def reset_states
        super
        reset_dataset
        reset_trans
        reset_trans_logs
      end

      def reset_dataset
        @dataset = {new: {}, pending: {}, failed: {}, archived: {}}
      end

      def reset_trans
        @trans = {new: {}, pending: {}, failed: {}, closed: {}}
      end

      def reset_trans_logs
        @trans_logs = {new: {}, failed: {}, closed: {}}
      end

      def post_init
        options[:primary_key] = options[:primary_key].to_sym
        super
        if options[:rename_attrs].has_key?(options[:trace_attr])
          options[:trace_attr] = options[:rename_attrs][options[:trace_attr]]
        end
      end

      def update_states
        update_dataset
        update_trans
        update_trans_logs
        super
      end

      %i[dataset trans trans_logs].each do |state|
        define_method("update_#{state}") do
          send(state).each_pair do |k, v|
            result.each { |r| v.merge!(r.send(state)[k]) }
          end
        end
      end

      def traceable_dataset
        dataset[:archived]
      end

      def no_more_traces?
        dataset[:new].empty?
      end

      def update_next_operation_for_resolve_pending
        @next_operation = :archive_new if pending_resolved?
      end

      def update_summary_for_resolve_pending
        if dataset[:archived].size > 0
          @summary += "Resolved #{dataset[:archived].size} pending records " \
            "(#{(dataset[:archived].size / stats[:elapsed_time]).to_i} rps). "
        end

        if dataset[:failed].size > 0
          @summary += "Failed to archive #{dataset[:failed].size} pending records."
        end

        @summary += if pending_resolved?
          "No more pending records are left. Switched to archive new records."
        else
          "Continue to resolve pending records."
        end
      end

      def update_next_operation_for_archive_new
        @next_operation = :resolve_pending if error?
      end

      def update_summary_for_archive_new
        if dataset[:archived].size > 0
          @summary += "Archived #{dataset[:archived].size} new records " \
            "(#{(dataset[:archived].size / stats[:elapsed_time]).to_i} rps). "
        end

        if dataset[:failed].size > 0
          @summary += "Failed to archive #{dataset[:failed].size} new records. "
        end

        @summary += if caught_up?
          "Data archival caught up: " \
            "synced at - #{trace[:synced_at]}; " \
            "archived_at - #{trace[:traced_at]}. " \
            "Wait for #{options[:wait]} secs before next run. "
        else
          "Continue to archive new records."
        end
      end
    end
  end
end
